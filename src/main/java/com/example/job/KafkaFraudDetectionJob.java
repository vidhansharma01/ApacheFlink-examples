package com.example.job;

import com.example.dto.FraudAlert;
import com.example.dto.TransactionEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Flink streaming job that reads raw transaction events from a Kafka topic,
 * detects fraud using {@link FraudDetectorProcessFunction}, and writes alerts
 * to a separate Kafka topic.
 *
 * <h3>Pipeline stages</h3>
 * <pre>
 *  Kafka (transactions)
 *      → parse JSON  ──(bad messages)──→ dead-letter log
 *      → assign event-time watermarks
 *      → keyBy(userId)
 *      → FraudDetectorProcessFunction
 *      → serialise to JSON
 *      → Kafka (fraud-alerts)
 *      → human-readable stdout log
 * </pre>
 *
 * <h3>Watermark strategy</h3>
 * <p>Uses {@link WatermarkStrategy#forBoundedOutOfOrderness} with a 10-second
 * tolerance. Without a real watermark strategy, event-time timers and windows
 * would never advance, silently breaking all time-based logic.
 *
 * <h3>Dead-letter handling</h3>
 * <p>Malformed JSON is routed to a Flink side output and printed, rather than
 * crashing the task and triggering costly restarts.
 */
@Component
public class KafkaFraudDetectionJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFraudDetectionJob.class);

    /**
     * Side-output tag for messages that fail JSON deserialisation.
     * Static so the tag identity is stable across serialisation boundaries.
     */
    static final OutputTag<String> DEAD_LETTER_TAG =
            new OutputTag<String>("dead-letter-transactions") {};

    private final StreamExecutionEnvironment env;
    private final String     bootstrapServers;
    private final String     transactionsTopic;
    private final String     alertsTopic;
    private final String     groupId;
    private final BigDecimal largeTransactionThreshold;
    private final BigDecimal rapidTotalThreshold;
    private final long       rollingWindowMs;

    public KafkaFraudDetectionJob(
            StreamExecutionEnvironment env,
            @Value("${fraud.job.bootstrap-servers}")         String bootstrapServers,
            @Value("${fraud.job.transactions-topic}")        String transactionsTopic,
            @Value("${fraud.job.alerts-topic}")              String alertsTopic,
            @Value("${fraud.job.group-id}")                  String groupId,
            @Value("${fraud.job.large-transaction-threshold}") BigDecimal largeTransactionThreshold,
            @Value("${fraud.job.rapid-total-threshold}")     BigDecimal rapidTotalThreshold,
            @Value("${fraud.job.rolling-window-ms:3600000}") long rollingWindowMs) {
        this.env                       = env;
        this.bootstrapServers          = bootstrapServers;
        this.transactionsTopic         = transactionsTopic;
        this.alertsTopic               = alertsTopic;
        this.groupId                   = groupId;
        this.largeTransactionThreshold = largeTransactionThreshold;
        this.rapidTotalThreshold       = rapidTotalThreshold;
        this.rollingWindowMs           = rollingWindowMs;
    }

    public void execute() throws Exception {
        LOG.info("Starting KafkaFraudDetectionJob — topic={} largeThreshold={} rapidThreshold={} windowMs={}",
                transactionsTopic, largeTransactionThreshold, rapidTotalThreshold, rollingWindowMs);

        // ------------------------------------------------------------------ //
        // 1. Source: read raw JSON strings from Kafka                         //
        // ------------------------------------------------------------------ //
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(transactionsTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        // ------------------------------------------------------------------ //
        // 2. Parse JSON → TransactionEvent, routing bad records to dead-letter//
        // ------------------------------------------------------------------ //
        // Watermarks are not assigned on the raw string stream because we have
        // no timestamp information yet. They are assigned after parsing.
        SingleOutputStreamOperator<TransactionEvent> parsedStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "transactions-kafka-source")
                .process(new TransactionJsonParserFunction(DEAD_LETTER_TAG))
                .name("parse-transactions");

        // Route dead-letter messages to stdout (replace with a dedicated Kafka
        // topic or S3 sink in a full production deployment).
        DataStream<String> deadLetterStream = parsedStream.getSideOutput(DEAD_LETTER_TAG);
        deadLetterStream.print("[DEAD-LETTER]");

        // ------------------------------------------------------------------ //
        // 3. Assign event-time watermarks                                     //
        // ------------------------------------------------------------------ //
        // forBoundedOutOfOrderness allows up to 10 s of late events before the
        // watermark advances. withIdleness prevents the watermark from stalling
        // when a Kafka partition receives no messages for > 1 minute.
        WatermarkStrategy<TransactionEvent> watermarkStrategy = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, ts) -> event.getEventTimestamp())
                .withIdleness(Duration.ofMinutes(1));

        DataStream<TransactionEvent> timedStream = parsedStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("assign-watermarks");

        // ------------------------------------------------------------------ //
        // 4. Fraud detection (keyed by userId)                                //
        // ------------------------------------------------------------------ //
        SingleOutputStreamOperator<FraudAlert> alerts = timedStream
                .keyBy(TransactionEvent::getUserId)
                .process(new FraudDetectorProcessFunction(
                        largeTransactionThreshold, rapidTotalThreshold, rollingWindowMs))
                .name("fraud-detector");

        // ------------------------------------------------------------------ //
        // 5. Sink: write alerts to Kafka                                      //
        // ------------------------------------------------------------------ //
        alerts
                .map(new FraudAlertJsonWriterFunction())
                .name("serialize-alerts")
                .sinkTo(buildAlertSink())
                .name("kafka-alert-sink");

        // ------------------------------------------------------------------ //
        // 6. Observability: structured log line per alert                     //
        // ------------------------------------------------------------------ //
        alerts
                .map(a -> String.format(
                        "[FraudAlert] severity=%s userId=%s txId=%s amount=%s reason=%s merchantId=%s",
                        a.getSeverity(), a.getUserId(), a.getTransactionId(),
                        a.getAmount(), a.getReason(), a.getMerchantId()))
                .name("alert-logger")
                .print();

        env.execute("kafka-fraud-detection-job");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private KafkaSink<String> buildAlertSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(alertsTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // AT_LEAST_ONCE matches the EXACTLY_ONCE checkpoint mode on the source.
                // Upgrade to EXACTLY_ONCE here if the Kafka broker supports transactions.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
