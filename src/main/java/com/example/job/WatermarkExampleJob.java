package com.example.job;

import com.example.dto.Purchase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
public class WatermarkExampleJob {

    private final StreamExecutionEnvironment env;

    public WatermarkExampleJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // Sample purchase events. The last record is intentionally out of order
        // to demonstrate how the watermark still allows slightly late events.
        DataStream<Purchase> purchaseStream = env.fromElements(
                new Purchase("u1", 120.0, LocalDateTime.of(2026, 4, 4, 11, 0, 0)),
                new Purchase("u1", 80.0, LocalDateTime.of(2026, 4, 4, 11, 0, 4)),
                new Purchase("u2", 250.0, LocalDateTime.of(2026, 4, 4, 11, 0, 3)),
                new Purchase("u1", 40.0, LocalDateTime.of(2026, 4, 4, 8, 50, 59))
        );

        // Watermarks tell Flink how far event time has progressed.
        // This strategy says events may arrive up to 5 seconds late.
        KeyedStream<Purchase, String> keyedStream = purchaseStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Purchase>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        event.getEventDateTime()
                                                .atZone(ZoneId.systemDefault())
                                                .toInstant()
                                                .toEpochMilli())
                )
                .keyBy(Purchase::getUserId);

        DataStream<String> totalSpendPerUser = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<Purchase, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String userId,
                            Context context,
                            Iterable<Purchase> elements,
                            Collector<String> out) {
                        double total = 0.0;
                        for (Purchase purchase : elements) {
                            total += purchase.getAmount();
                        }

                        out.collect("userId=" + userId + ", totalSpend=" + total);
                    }
                });

        totalSpendPerUser.print("[WatermarkExample]");
        env.execute("watermark-example-job");
    }
}
