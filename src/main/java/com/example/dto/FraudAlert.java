package com.example.dto;

public class FraudAlert {
    private String transactionId;
    private String userId;
    private double amount;
    private String reason;
    private String createdAt;

    public FraudAlert() {
    }

    public FraudAlert(String transactionId, String userId, double amount, String reason, String createdAt) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.reason = reason;
        this.createdAt = createdAt;
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

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
}
