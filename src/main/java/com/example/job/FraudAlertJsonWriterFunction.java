package com.example.job;

import com.example.dto.FraudAlert;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Serialises a {@link FraudAlert} to a JSON string for writing to Kafka.
 *
 * <p>{@link ObjectMapper} is created once per task instance in {@link #open}
 * (thread-safe after construction) instead of being recreated per record.
 */
public class FraudAlertJsonWriterFunction extends RichMapFunction<FraudAlert, String> {

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public String map(FraudAlert alert) throws Exception {
        return mapper.writeValueAsString(alert);
    }
}
