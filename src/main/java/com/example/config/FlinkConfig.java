package com.example.config;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Spring configuration that produces a fully configured Flink
 * {@link StreamExecutionEnvironment} bean.
 *
 * <h3>Checkpoint directory resolution</h3>
 * <p>The directory is read from {@code flink.checkpoint.dir} and supports any
 * URI scheme understood by Flink (e.g., {@code file://}, {@code s3://},
 * {@code hdfs://}). If the property is absent or blank, the JVM temp directory
 * is used as a safe local fallback — this avoids the previous hard-coded
 * {@code C:\flink-demo-checkpoints} path that broke on Linux cluster deployments.
 */
@Configuration
public class FlinkConfig {

    /**
     * Checkpoint storage URI. Supports {@code file://}, {@code s3://}, {@code hdfs://}.
     * Leave blank to fall back to {@code <java.io.tmpdir>/flink-checkpoints}.
     */
    @Value("${flink.checkpoint.dir:}")
    private String checkpointDir;

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String resolvedCheckpointDir = resolveCheckpointDir();

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(resolvedCheckpointDir);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000));

        return env;
    }

    /**
     * Returns the configured checkpoint URI, or a safe OS-temp fallback.
     * The fallback creates {@code <tmpdir>/flink-checkpoints} so it works on
     * both Windows and Linux without any manual directory creation.
     */
    private String resolveCheckpointDir() throws IOException {
        if (checkpointDir != null && !checkpointDir.isBlank()) {
            return checkpointDir;
        }
        Path localFallback = Path.of(System.getProperty("java.io.tmpdir"), "flink-checkpoints");
        Files.createDirectories(localFallback);
        return localFallback.toAbsolutePath().toUri().toString();
    }
}
