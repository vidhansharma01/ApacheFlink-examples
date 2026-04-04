package com.example.config;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfig {

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        // Create the Flink execution environment that Spring will inject
        // into our job class. In local development this creates a local
        // Flink runtime inside the same JVM as the Spring Boot app.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Run the bounded input as a batch-style job. This is a better fit
        // for a fixed set of lines because the pipeline can complete on its own
        // once all records have been processed.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Force a single parallel task so console output is easier to read
        // and deterministic during local debugging.
        env.setParallelism(1);
        return env;
    }
}
