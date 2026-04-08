package com.example.dto;

import java.math.BigDecimal;

/**
 * Represents a raw financial transaction consumed from Kafka.
 *
 * <p>Key design notes:
 * <ul>
 *   <li>{@code amount} is {@link BigDecimal} — {@code double} must never be used
 *       for monetary values due to floating-point precision loss.</li>
 *   <li>{@code eventTimestamp} is epoch-milliseconds rather than an ISO-8601 string.
 *       This avoids per-record {@code Instant.parse()} calls and integrates directly
 *       with Flink's {@code TimestampAssigner}.</li>
 * </ul>
 */
public class TransactionEvent {

    private String transactionId;
    private String userId;
    private BigDecimal amount;
    private String merchantId;
    /** Epoch-milliseconds at which the transaction occurred (event time). */
    private long eventTimestamp;

    public TransactionEvent() {}

    public TransactionEvent(String transactionId, String userId, BigDecimal amount,
                            String merchantId, long eventTimestamp) {
        this.transactionId  = transactionId;
        this.userId         = userId;
        this.amount         = amount;
        this.merchantId     = merchantId;
        this.eventTimestamp = eventTimestamp;
    }

    public String getTransactionId() { return transactionId; }
    public void   setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getUserId() { return userId; }
    public void   setUserId(String userId) { this.userId = userId; }

    public BigDecimal getAmount() { return amount; }
    public void       setAmount(BigDecimal amount) { this.amount = amount; }

    public String getMerchantId() { return merchantId; }
    public void   setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    @Override
    public String toString() {
        return "TransactionEvent{txId='" + transactionId + "', userId='" + userId
                + "', amount=" + amount + ", merchantId='" + merchantId
                + "', eventTimestamp=" + eventTimestamp + '}';
    }
}
