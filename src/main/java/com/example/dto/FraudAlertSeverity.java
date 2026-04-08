package com.example.dto;

/**
 * Severity classification for fraud alerts.
 * Allows downstream consumers (alert routing, PagerDuty, dashboards) to
 * prioritize investigation without re-reading raw amounts.
 */
public enum FraudAlertSeverity {

    /** Informational — likely a false positive, log only. */
    LOW,

    /** Warrants async review within the business day. */
    MEDIUM,

    /** Warrants prompt human review (e.g., within 1 hour). */
    HIGH,

    /** Requires immediate action; transaction should be blocked if possible. */
    CRITICAL
}
