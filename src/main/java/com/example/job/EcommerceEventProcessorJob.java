package com.example.job;

import com.example.dto.EcommerceEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;

@Component
public class EcommerceEventProcessorJob {

    private static final OutputTag<String> HIGH_VALUE_USERS_TAG = new OutputTag<>("high-value-users") {
    };

    private final StreamExecutionEnvironment env;

    public EcommerceEventProcessorJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // Sample e-commerce input. Only purchase events should contribute to the
        // rolling spend total, while view/cart events are filtered out.
        DataStream<EcommerceEvent> events = env.fromElements(
                new EcommerceEvent("user-101", "item-1", "view", 0.0),
                new EcommerceEvent("user-101", "item-1", "purchase", 12000.0),
                new EcommerceEvent("user-202", "item-2", "purchase", 18000.0),
                new EcommerceEvent("user-101", "item-3", "cart", 0.0),
                new EcommerceEvent("user-101", "item-3", "purchase", 41000.0),
                new EcommerceEvent("user-202", "item-4", "purchase", 34000.0)
        );

        // 1. Keep only purchase events.
        DataStream<EcommerceEvent> purchases = events
                .filter(event -> "purchase".equalsIgnoreCase(event.getEventType()));

        // 2. Extract the fields needed for spend aggregation.
        DataStream<Tuple2<String, Double>> purchaseAmounts = purchases
                .map(new MapFunction<EcommerceEvent, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(EcommerceEvent event) {
                        return Tuple2.of(event.getUserId(), event.getAmount());
                    }
                });

        // 3. Key the stream by user id so each user's spend is aggregated independently.
        KeyedStream<Tuple2<String, Double>, String> keyedByUser = purchaseAmounts
                .keyBy(value -> value.f0);

        // 4. Maintain a rolling total spend per user.
        DataStream<Tuple2<String, Double>> totalSpendPerUser = keyedByUser
                .reduce((left, right) -> Tuple2.of(left.f0, left.f1 + right.f1));

        // 5. Side output high-value users whose rolling spend exceeds 50,000.
        SingleOutputStreamOperator<String> processedTotals = totalSpendPerUser
                .process(new ProcessFunction<Tuple2<String, Double>, String>() {
                    @Override
                    public void processElement(
                            Tuple2<String, Double> value,
                            Context ctx,
                            Collector<String> out) {
                        String mainMessage = "userId=" + value.f0 + ", totalSpend=" + value.f1;
                        out.collect(mainMessage);

                        if (value.f1 > 50000.0) {
                            ctx.output(
                                    HIGH_VALUE_USERS_TAG,
                                    "HIGH_VALUE userId=" + value.f0 + ", totalSpend=" + value.f1);
                        }
                    }
                });

        // 6. Print both the main stream and the side output.
        processedTotals.print("[Ecommerce-Main]");
        processedTotals.getSideOutput(HIGH_VALUE_USERS_TAG).print("[Ecommerce-HighValue]");

        env.execute("ecommerce-event-processor-job");
    }
}
