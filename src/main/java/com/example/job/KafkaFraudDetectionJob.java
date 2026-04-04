package com.example.job;

import com.example.dto.FraudAlert;
import com.example.dto.TransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.io.Serializable;

@Component
public class KafkaFraudDetectionJob {

    private final StreamExecutionEnvironment env;
    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String transactionsTopic;
    private final String alertsTopic;
    private final String groupId;
    private final double largeTransactionThreshold;
    private final double rapidTotalThreshold;

    public KafkaFraudDetectionJob(
            StreamExecutionEnvironment env,
            @Value("${fraud.job.bootstrap-servers}") String bootstrapServers,
            @Value("${fraud.job.transactions-topic}") String transactionsTopic,
            @Value("${fraud.job.alerts-topic}") String alertsTopic,
            @Value("${fraud.job.group-id}") String groupId,
            @Value("${fraud.job.large-transaction-threshold}") double largeTransactionThreshold,
            @Value("${fraud.job.rapid-total-threshold}") double rapidTotalThreshold) {
        this.env = env;
        this.objectMapper = new ObjectMapper();
        this.bootstrapServers = bootstrapServers;
        this.transactionsTopic = transactionsTopic;
        this.alertsTopic = alertsTopic;
        this.groupId = groupId;
        this.largeTransactionThreshold = largeTransactionThreshold;
        this.rapidTotalThreshold = rapidTotalThreshold;
    }

    public void execute() throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(transactionsTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        DataStream<TransactionEvent> transactions = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "transactions-kafka-source")
                .map(new TransactionJsonParser());

        DataStream<FraudAlert> alerts = transactions
                .keyBy(TransactionEvent::getUserId)
                .process(new FraudDetectorProcessFunction(
                        largeTransactionThreshold,
                        rapidTotalThreshold));

        alerts.map(new FraudAlertJsonWriter())
                .sinkTo(createAlertSink());

        alerts.map(new FraudAlertLogFormatter())
                .print();

        env.execute("kafka-fraud-detection-job");
    }

    private Sink<String> createAlertSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(alertsTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static final class TransactionJsonParser implements MapFunction<String, TransactionEvent>, Serializable {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public TransactionEvent map(String json) throws Exception {
            return mapper.readValue(json, TransactionEvent.class);
        }
    }

    private static final class FraudAlertJsonWriter implements MapFunction<FraudAlert, String>, Serializable {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String map(FraudAlert alert) throws Exception {
            return mapper.writeValueAsString(alert);
        }
    }

    private static final class FraudAlertLogFormatter implements MapFunction<FraudAlert, String>, Serializable {
        @Override
        public String map(FraudAlert alert) {
            return "[FraudAlert] userId=" + alert.getUserId()
                    + ", transactionId=" + alert.getTransactionId()
                    + ", amount=" + alert.getAmount()
                    + ", reason=" + alert.getReason();
        }
    }

    private static final class FraudDetectorProcessFunction
            extends KeyedProcessFunction<String, TransactionEvent, FraudAlert> {

        private final double largeTransactionThreshold;
        private final double rapidTotalThreshold;
        private transient ValueState<Double> rollingSpendState;

        private FraudDetectorProcessFunction(double largeTransactionThreshold, double rapidTotalThreshold) {
            this.largeTransactionThreshold = largeTransactionThreshold;
            this.rapidTotalThreshold = rapidTotalThreshold;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            rollingSpendState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("rolling-spend", Double.class));
        }

        @Override
        public void processElement(
                TransactionEvent value,
                Context ctx,
                Collector<FraudAlert> out) throws Exception {
            Double currentTotal = rollingSpendState.value();
            if (currentTotal == null) {
                currentTotal = 0.0;
            }

            double updatedTotal = currentTotal + value.getAmount();
            rollingSpendState.update(updatedTotal);

            if (value.getAmount() >= largeTransactionThreshold) {
                out.collect(new FraudAlert(
                        value.getTransactionId(),
                        value.getUserId(),
                        value.getAmount(),
                        "LARGE_TRANSACTION",
                        Instant.now().toString()));
            } else if (updatedTotal >= rapidTotalThreshold) {
                out.collect(new FraudAlert(
                        value.getTransactionId(),
                        value.getUserId(),
                        value.getAmount(),
                        "ROLLING_SPEND_THRESHOLD_EXCEEDED",
                        Instant.now().toString()));
            }
        }
    }
}
