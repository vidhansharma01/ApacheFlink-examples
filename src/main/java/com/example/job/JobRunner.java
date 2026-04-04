package com.example.job;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class JobRunner implements ApplicationRunner {

    // Only one demo job should execute at startup because each job submits the
    // Flink environment for execution.
    private final WatermarkExampleJob watermarkExampleJob;

    public JobRunner(WatermarkExampleJob watermarkExampleJob) {
        this.watermarkExampleJob = watermarkExampleJob;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // ApplicationRunner is a good place for demo jobs like this because
        // it runs once at startup and keeps the bootstrapping logic out of
        // the main application class.
        watermarkExampleJob.execute();
    }
}
