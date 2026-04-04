package com.example.job;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class JobRunner implements ApplicationRunner {

    // Spring injects the Flink word count job here so we can trigger it
    // automatically after the application context has started successfully.
    private final ProcessFunctionExample processFunctionExample;
    private final WordCountJob wordCountJob;

    public JobRunner(ProcessFunctionExample processFunctionExample, WordCountJob wordCountJob) {
        this.processFunctionExample = processFunctionExample;
        this.wordCountJob = wordCountJob;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // ApplicationRunner is a good place for demo jobs like this because
        // it runs once at startup and keeps the bootstrapping logic out of
        // the main application class.
        processFunctionExample.explainExample();
        wordCountJob.execute();
    }
}
