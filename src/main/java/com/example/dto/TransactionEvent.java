package com.example.dto;

public class TransactionEvent {
    private String transactionId;
    private String userId;
    private double amount;
    private String merchantId;
    private String eventTime;

    public TransactionEvent() {
    }

    public TransactionEvent(String transactionId, String userId, double amount, String merchantId, String eventTime) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.merchantId = merchantId;
        this.eventTime = eventTime;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }
}
