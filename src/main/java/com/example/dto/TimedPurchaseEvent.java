package com.example.dto;

import java.time.LocalDateTime;

public class TimedPurchaseEvent {
    private String userId;
    private String itemId;
    private String eventType;
    private double amount;
    private String eventTime;

    public TimedPurchaseEvent() {
    }

    public TimedPurchaseEvent(String userId, String itemId, String eventType, double amount, LocalDateTime eventTime) {
        this.userId = userId;
        this.itemId = itemId;
        this.eventType = eventType;
        this.amount = amount;
        this.eventTime = eventTime.toString();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
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
