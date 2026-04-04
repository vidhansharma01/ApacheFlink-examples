package com.example.dto;

public class EcommerceEvent {
    private String userId;
    private String itemId;
    private String eventType;
    private double amount;

    public EcommerceEvent() {
    }

    public EcommerceEvent(String userId, String itemId, String eventType, double amount) {
        this.userId = userId;
        this.itemId = itemId;
        this.eventType = eventType;
        this.amount = amount;
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
}
