package com.example.dto;

import java.time.LocalDateTime;

public class Purchase {
    // Public no-arg constructor + non-final fields make this a Flink POJO.
    // That allows Flink to use its POJO serializer instead of falling back
    // to Kryo, which is what triggered the Java 17 reflective access error.
    private String userId;
    private double amount;
    private String eventTime;

    public Purchase() {
    }

    public Purchase(String userId, double amount, LocalDateTime eventTime) {
        this.userId = userId;
        this.amount = amount;
        this.eventTime = eventTime.toString();
    }

    public Purchase(String userId, double amount, String eventTime) {
        this.userId = userId;
        this.amount = amount;
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public LocalDateTime getEventDateTime() {
        return LocalDateTime.parse(eventTime);
    }
}
