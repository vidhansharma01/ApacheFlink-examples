package com.example.job;

import com.example.dto.TransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses raw JSON strings into {@link TransactionEvent} objects.
 *
 * <p>Malformed or unparseable messages are routed to a side output (dead-letter
 * stream) rather than crashing the task. This prevents a single bad Kafka message
 * from triggering repeated task restarts and avoids data loss of valid messages
 * that follow it in the same partition.
 *
 * <p>The {@link ObjectMapper} is initialised once per task instance in
 * {@link #open(Configuration)} rather than per-record, avoiding the significant
 * overhead of repeated construction.
 */
public class TransactionJsonParserFunction extends ProcessFunction<String, TransactionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionJsonParserFunction.class);

    private final OutputTag<String> deadLetterTag;
    private transient ObjectMapper  mapper;

    public TransactionJsonParserFunction(OutputTag<String> deadLetterTag) {
        this.deadLetterTag = deadLetterTag;
    }

    @Override
    public void open(Configuration parameters) {
        // ObjectMapper is thread-safe for reads after configuration; creating it once
        // per task instance (not per record) avoids repeated expensive construction.
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(String json, Context ctx, Collector<TransactionEvent> out) {
        try {
            out.collect(mapper.readValue(json, TransactionEvent.class));
        } catch (Exception e) {
            LOG.warn("Failed to deserialise transaction JSON — routing to dead-letter. payload={}", json, e);
            ctx.output(deadLetterTag, json);
        }
    }
}
