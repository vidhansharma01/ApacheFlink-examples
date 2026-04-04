package com.example.job;

import com.example.dto.Event;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

@Component
public class EventDeduplicationJob {

    private final StreamExecutionEnvironment env;

    public EventDeduplicationJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // This sample stream is bounded, so batch mode makes the local demo
        // finish cleanly after all records are processed.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Event> events = env.fromElements(
                new Event("evt-1001", "user-101", "opened-home-page"),
                new Event("evt-1002", "user-202", "added-to-cart"),
                new Event("evt-1001", "user-101", "opened-home-page"),
                new Event("evt-1003", "user-101", "purchase-complete"),
                new Event("evt-1002", "user-202", "added-to-cart"),
                new Event("evt-1004", "user-303", "search-query")
        );

        // Key by event id so each logical event gets its own piece of state.
        KeyedStream<Event, String> keyedByEventId = events.keyBy(Event::getEventId);

        DataStream<Event> uniqueEvents = keyedByEventId.process(new DeduplicationFunction());

        uniqueEvents.print("[DeduplicatedEvents]");
        env.execute("event-deduplication-job");
    }

    public static class DeduplicationFunction extends KeyedProcessFunction<String, Event, Event> {

        // Because the stream is already keyed by event id, one boolean per key
        // is enough to know whether we have seen that event before.
        private transient ValueState<Boolean> seenState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();

            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("seen-event", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);
            seenState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
            Boolean seen = seenState.value();

            if (seen == null) {
                // First time this event id appears, so mark it as seen and emit it.
                seenState.update(Boolean.TRUE);
                out.collect(event);
            }
            // Duplicate events are intentionally dropped silently.
        }
    }
}
