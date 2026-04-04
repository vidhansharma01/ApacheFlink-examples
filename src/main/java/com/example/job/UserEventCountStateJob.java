package com.example.job;

import com.example.dto.UserEvent;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

@Component
public class UserEventCountStateJob {

    private final StreamExecutionEnvironment env;

    public UserEventCountStateJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // This example is bounded and finishes on its own, so batch mode keeps
        // the local run simple while still allowing keyed state.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Sample user events. The same user appears multiple times so we can
        // observe keyed state being updated independently per user.
        DataStream<UserEvent> events = env.fromElements(
                new UserEvent("user-101", "view"),
                new UserEvent("user-202", "cart"),
                new UserEvent("user-101", "purchase"),
                new UserEvent("user-101", "view"),
                new UserEvent("user-202", "purchase"),
                new UserEvent("user-303", "view"),
                new UserEvent("user-202", "view")
        );

        // Key the stream by user id so each key gets its own isolated state.
        KeyedStream<UserEvent, String> keyedByUser = events.keyBy(UserEvent::getUserId);

        DataStream<String> countsPerUser = keyedByUser
                .process(new KeyedProcessFunction<String, UserEvent, String>() {

                    // ValueState holds one counter per user key.
                    private transient ValueState<Long> eventCountState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Long> descriptor =
                                new ValueStateDescriptor<>("event-count", Long.class);
                        eventCountState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(
                            UserEvent value,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        Long currentCount = eventCountState.value();
                        if (currentCount == null) {
                            currentCount = 0L;
                        }

                        long updatedCount = currentCount + 1;
                        eventCountState.update(updatedCount);

                        out.collect(
                                "userId=" + value.getUserId()
                                        + ", eventType=" + value.getEventType()
                                        + ", totalEvents=" + updatedCount);
                    }
                });

        countsPerUser.print("[UserEventState]");
        env.execute("user-event-count-state-job");
    }
}
