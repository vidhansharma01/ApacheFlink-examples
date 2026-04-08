package com.example.dto;

/**
 * Typed enumeration of all fraud detection reasons.
 * Using an enum instead of a raw String prevents typos, enables exhaustive
 * switch expressions, and makes alert routing/filtering safer downstream.
 */
public enum FraudReason {

    /** A single transaction exceeds the configured large-transaction threshold. */
    LARGE_TRANSACTION,

    /** A user's cumulative spend within the rolling window exceeded the threshold. */
    ROLLING_SPEND_THRESHOLD_EXCEEDED
}
