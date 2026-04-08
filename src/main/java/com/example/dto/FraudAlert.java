package com.example.dto;

import java.math.BigDecimal;

/**
 * Represents a detected fraud alert emitted by the Flink pipeline.
 *
 * <p>Key design notes:
 * <ul>
 *   <li>{@code reason} is a typed {@link FraudReason} enum — not a raw String — so
 *       downstream consumers can switch on it safely.</li>
 *   <li>{@code severity} allows alert-routing systems (PagerDuty, dashboards) to
 *       triage without re-evaluating raw amounts.</li>
 *   <li>{@code eventTimestamp} is sourced from the originating transaction, NOT from
 *       {@code Instant.now()}, ensuring correct audit timestamps during replays.</li>
 *   <li>{@code detectedAt} records the processing wall-clock time for latency tracking.</li>
 *   <li>{@code merchantId} is preserved from the transaction so analysts can identify
 *       card-testing patterns across merchants.</li>
 * </ul>
 */
public class FraudAlert {

    private String             transactionId;
    private String             userId;
    private BigDecimal         amount;
    private String             merchantId;
    private FraudReason        reason;
    private FraudAlertSeverity severity;
    /** Epoch-millis from the originating transaction (event time). */
    private long               eventTimestamp;
    /** Epoch-millis when the alert was produced (processing time). */
    private long               detectedAt;

    public FraudAlert() {}

    public FraudAlert(String transactionId, String userId, BigDecimal amount,
                      String merchantId, FraudReason reason, FraudAlertSeverity severity,
                      long eventTimestamp, long detectedAt) {
        this.transactionId  = transactionId;
        this.userId         = userId;
        this.amount         = amount;
        this.merchantId     = merchantId;
        this.reason         = reason;
        this.severity       = severity;
        this.eventTimestamp = eventTimestamp;
        this.detectedAt     = detectedAt;
    }

    public String             getTransactionId()  { return transactionId; }
    public void               setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String             getUserId()         { return userId; }
    public void               setUserId(String userId) { this.userId = userId; }

    public BigDecimal         getAmount()         { return amount; }
    public void               setAmount(BigDecimal amount) { this.amount = amount; }

    public String             getMerchantId()     { return merchantId; }
    public void               setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public FraudReason        getReason()         { return reason; }
    public void               setReason(FraudReason reason) { this.reason = reason; }

    public FraudAlertSeverity getSeverity()        { return severity; }
    public void               setSeverity(FraudAlertSeverity severity) { this.severity = severity; }

    public long               getEventTimestamp() { return eventTimestamp; }
    public void               setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long               getDetectedAt()     { return detectedAt; }
    public void               setDetectedAt(long detectedAt) { this.detectedAt = detectedAt; }

    @Override
    public String toString() {
        return "FraudAlert{txId='" + transactionId + "', userId='" + userId
                + "', amount=" + amount + ", merchantId='" + merchantId
                + "', reason=" + reason + ", severity=" + severity
                + ", eventTimestamp=" + eventTimestamp + ", detectedAt=" + detectedAt + '}';
    }
}
