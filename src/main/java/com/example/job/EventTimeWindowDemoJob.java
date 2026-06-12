package com.example.job;

import com.example.dto.TimedPurchaseEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
public class EventTimeWindowDemoJob {

    private static final OutputTag<TimedPurchaseEvent> LATE_EVENTS_TAG =
            new OutputTag<>("late-events", TypeInformation.of(TimedPurchaseEvent.class));

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final StreamExecutionEnvironment env;

    public EventTimeWindowDemoJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // This demo focuses on event-time behavior, so we force streaming mode
        // even though the source is bounded and runs locally.
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<TimedPurchaseEvent> source = env.fromElements(
                new TimedPurchaseEvent("user-101", "item-1", "purchase", 120.0, LocalDateTime.of(2026, 4, 4, 12, 0, 5)),
                new TimedPurchaseEvent("user-101", "item-2", "purchase", 180.0, LocalDateTime.of(2026, 4, 4, 12, 0, 25)),
                new TimedPurchaseEvent("user-202", "item-3", "purchase", 300.0, LocalDateTime.of(2026, 4, 4, 12, 0, 40)),
                new TimedPurchaseEvent("user-101", "item-4", "purchase", 210.0, LocalDateTime.of(2026, 4, 4, 12, 1, 12)),
                // This event belongs to the 12:00 window but arrives after the
                // watermark has advanced beyond windowEnd + allowedLateness.
                new TimedPurchaseEvent("user-101", "item-late", "purchase", 999.0, LocalDateTime.of(2026, 4, 4, 12, 0, 50)),
                new TimedPurchaseEvent("user-101", "item-5", "purchase", 150.0, LocalDateTime.of(2026, 4, 4, 12, 2, 0)),
                new TimedPurchaseEvent("user-101", "item-6", "purchase", 90.0, LocalDateTime.of(2026, 4, 4, 12, 2, 20)),
                // Gap of 40 seconds from the previous user-101 event; this
                // forces a new session when session gap is 30 seconds.
                new TimedPurchaseEvent("user-101", "item-7", "purchase", 110.0, LocalDateTime.of(2026, 4, 4, 12, 3, 0)),
                // Advance the watermark far enough that all prior windows close.
                new TimedPurchaseEvent("user-999", "item-end", "purchase", 10.0, LocalDateTime.of(2026, 4, 4, 12, 6, 30))
        );

        DataStream<TimedPurchaseEvent> purchaseEvents = source
                .filter(event -> "purchase".equalsIgnoreCase(event.getEventType()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimedPurchaseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        event.getEventDateTime()
                                                .atZone(ZoneId.systemDefault())
                                                .toInstant()
                                                .toEpochMilli())
                );

        KeyedStream<TimedPurchaseEvent, String> keyedByUser = purchaseEvents
                .keyBy(TimedPurchaseEvent::getUserId);

        // 1-minute tumbling event-time window: count events per user per minute.
        SingleOutputStreamOperator<String> perMinuteCounts = keyedByUser
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(LATE_EVENTS_TAG)
                .process(new ProcessWindowFunction<TimedPurchaseEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String userId,
                            Context context,
                            Iterable<TimedPurchaseEvent> elements,
                            Collector<String> out) {
                        long count = 0L;
                        for (TimedPurchaseEvent ignored : elements) {
                            count++;
                        }

                        out.collect(
                                "TUMBLING userId=" + userId
                                        + ", window=" + formatWindow(context.window())
                                        + ", count=" + count);
                    }
                });

        // 5-minute sliding window with 1-minute slide: rolling average amount.
        DataStream<String> rollingAverages = keyedByUser
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(
                        new AggregateFunction<TimedPurchaseEvent, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                            @Override
                            public Tuple2<Double, Long> createAccumulator() {
                                return Tuple2.of(0.0, 0L);
                            }

                            @Override
                            public Tuple2<Double, Long> add(
                                    TimedPurchaseEvent value,
                                    Tuple2<Double, Long> accumulator) {
                                return Tuple2.of(accumulator.f0 + value.getAmount(), accumulator.f1 + 1);
                            }

                            @Override
                            public Tuple2<Double, Long> getResult(Tuple2<Double, Long> accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Tuple2<Double, Long> merge(
                                    Tuple2<Double, Long> left,
                                    Tuple2<Double, Long> right) {
                                return Tuple2.of(left.f0 + right.f0, left.f1 + right.f1);
                            }
                        },
                        new ProcessWindowFunction<Tuple2<Double, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String userId,
                                    Context context,
                                    Iterable<Tuple2<Double, Long>> elements,
                                    Collector<String> out) {
                                Tuple2<Double, Long> aggregate = elements.iterator().next();
                                double average = aggregate.f1 == 0 ? 0.0 : aggregate.f0 / aggregate.f1;
                                out.collect(
                                        "SLIDING_AVG userId=" + userId
                                                + ", window=" + formatWindow(context.window())
                                                + ", averageAmount=" + average);
                            }
                        });

        // Session window with 30-second inactivity gap.
        DataStream<String> sessionSummaries = keyedByUser
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .process(new ProcessWindowFunction<TimedPurchaseEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String userId,
                            Context context,
                            Iterable<TimedPurchaseEvent> elements,
                            Collector<String> out) {
                        long count = 0L;
                        double totalAmount = 0.0;
                        for (TimedPurchaseEvent event : elements) {
                            count++;
                            totalAmount += event.getAmount();
                        }

                        out.collect(
                                "SESSION userId=" + userId
                                        + ", session=" + formatWindow(context.window())
                                        + ", events=" + count
                                        + ", totalAmount=" + totalAmount);
                    }
                });

        perMinuteCounts.print("[PerMinuteCount]");
        rollingAverages.print("[SlidingAverage]");
        sessionSummaries.print("[SessionWindow]");

        perMinuteCounts.getSideOutput(LATE_EVENTS_TAG)
                .map(new MapFunction<TimedPurchaseEvent, String>() {
                    @Override
                    public String map(TimedPurchaseEvent event) {
                        return "LATE_EVENT userId=" + event.getUserId()
                                + ", itemId=" + event.getItemId()
                                + ", amount=" + event.getAmount()
                                + ", eventTime=" + event.getEventTime()
                                + " -> exceeded allowed lateness and went to side output";
                    }
                })
                .returns(TypeInformation.of(new TypeHint<String>() {
                }))
                .print("[LateEvents]");

        env.execute("event-time-window-demo-job");
    }

    private static String formatWindow(TimeWindow window) {
        return formatEpochMillis(window.getStart()) + " to " + formatEpochMillis(window.getEnd());
    }

    private static String formatEpochMillis(long epochMillis) {
        return LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(epochMillis),
                ZoneId.systemDefault()).format(FORMATTER);
    }
}
