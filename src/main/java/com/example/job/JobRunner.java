package com.example.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Bootstraps the Flink streaming job on Spring application startup.
 *
 * <p>{@link ApplicationRunner} is the correct hook for a long-running job because
 * it blocks the main thread intentionally, keeping the JVM alive while Flink
 * is executing. Errors are caught, logged with full context, and re-thrown as an
 * {@link IllegalStateException} so Spring's exit-code mechanism can propagate
 * a non-zero exit code to the process supervisor / Kubernetes restart policy.
 */
@Component
public class JobRunner implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);

    private final KafkaFraudDetectionJob kafkaFraudDetectionJob;

    public JobRunner(KafkaFraudDetectionJob kafkaFraudDetectionJob) {
        this.kafkaFraudDetectionJob = kafkaFraudDetectionJob;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            LOG.info("Submitting KafkaFraudDetectionJob to Flink runtime...");
            kafkaFraudDetectionJob.execute();
            LOG.info("KafkaFraudDetectionJob completed normally.");
        } catch (Exception e) {
            LOG.error("KafkaFraudDetectionJob terminated with an unrecoverable error — triggering shutdown.", e);
            // Re-throw so Spring Boot exits with a non-zero code, enabling
            // Kubernetes / systemd to restart the pod/service automatically.
            throw new IllegalStateException("Flink job terminated unexpectedly", e);
        }
    }
}
