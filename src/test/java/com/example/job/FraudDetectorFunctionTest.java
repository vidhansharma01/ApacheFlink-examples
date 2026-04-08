package com.example.job;

import com.example.dto.FraudAlert;
import com.example.dto.FraudAlertSeverity;
import com.example.dto.FraudReason;
import com.example.dto.TransactionEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FraudDetectorProcessFunction}.
 *
 * <p>Uses Flink's {@link KeyedOneInputStreamOperatorTestHarness} to drive the
 * operator in isolation — no MiniCluster, no Kafka, no Spring context required.
 * This makes the tests fast (< 1 s each) and deterministic.
 */
class FraudDetectorFunctionTest {

    private static final BigDecimal LARGE_THRESHOLD  = new BigDecimal("50000");
    private static final BigDecimal RAPID_THRESHOLD  = new BigDecimal("70000");
    private static final long       WINDOW_MS        = 3_600_000L; // 1 hour

    private KeyedOneInputStreamOperatorTestHarness<String, TransactionEvent, FraudAlert> harness;

    @BeforeEach
    void setUp() throws Exception {
        FraudDetectorProcessFunction function =
                new FraudDetectorProcessFunction(LARGE_THRESHOLD, RAPID_THRESHOLD, WINDOW_MS);

        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                TransactionEvent::getUserId,
                Types.STRING);

        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    // -------------------------------------------------------------------------
    // Large transaction
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Single transaction >= largeThreshold emits HIGH alert")
    void largeTransaction_emitsHighAlert() throws Exception {
        TransactionEvent tx = transaction("tx-1", "user-1", "60000", "merchant-A");

        harness.processElement(tx, tx.getEventTimestamp());

        List<FraudAlert> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getReason()).isEqualTo(FraudReason.LARGE_TRANSACTION);
        assertThat(output.get(0).getSeverity()).isEqualTo(FraudAlertSeverity.HIGH);
        assertThat(output.get(0).getUserId()).isEqualTo("user-1");
        assertThat(output.get(0).getMerchantId()).isEqualTo("merchant-A");
        // eventTimestamp must come from the transaction, not wall-clock
        assertThat(output.get(0).getEventTimestamp()).isEqualTo(tx.getEventTimestamp());
    }

    @Test
    @DisplayName("Transaction >= 2x largeThreshold emits CRITICAL alert")
    void veryLargeTransaction_emitsCriticalAlert() throws Exception {
        TransactionEvent tx = transaction("tx-2", "user-2", "101000", "merchant-B");

        harness.processElement(tx, tx.getEventTimestamp());

        List<FraudAlert> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getSeverity()).isEqualTo(FraudAlertSeverity.CRITICAL);
    }

    @Test
    @DisplayName("Transaction exactly at largeThreshold emits HIGH alert")
    void transactionAtExactThreshold_emitsAlert() throws Exception {
        TransactionEvent tx = transaction("tx-3", "user-3", "50000", "merchant-C");

        harness.processElement(tx, tx.getEventTimestamp());

        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(harness.extractOutputValues().get(0).getReason())
                .isEqualTo(FraudReason.LARGE_TRANSACTION);
    }

    // -------------------------------------------------------------------------
    // Rolling spend threshold
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Multiple transactions crossing rapidThreshold emits MEDIUM alert")
    void rollingSpend_exceedsRapidThreshold_emitsMediumAlert() throws Exception {
        // 3 × $25,000 = $75,000 which crosses the $70,000 rapid threshold
        for (int i = 1; i <= 3; i++) {
            harness.processElement(transaction("tx-" + i, "user-4", "25000", "merchant-D"),
                    System.currentTimeMillis());
        }

        List<FraudAlert> output = harness.extractOutputValues();
        // Only the transaction that pushes total over the threshold generates an alert
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getReason()).isEqualTo(FraudReason.ROLLING_SPEND_THRESHOLD_EXCEEDED);
        assertThat(output.get(0).getSeverity()).isEqualTo(FraudAlertSeverity.MEDIUM);
    }

    @Test
    @DisplayName("Spend below both thresholds produces no alert")
    void normalSpend_producesNoAlert() throws Exception {
        harness.processElement(transaction("tx-10", "user-5", "100", "merchant-E"),
                System.currentTimeMillis());
        harness.processElement(transaction("tx-11", "user-5", "200", "merchant-E"),
                System.currentTimeMillis());

        assertThat(harness.extractOutputValues()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Rolling window reset via timer
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("State is cleared after rolling window expires via processing-time timer")
    void rollingWindow_resetsAfterTimer() throws Exception {
        // Accumulate spend that nears (but does not exceed) the rapid threshold
        harness.processElement(transaction("tx-20", "user-6", "35000", "merchant-F"),
                System.currentTimeMillis());
        harness.processElement(transaction("tx-21", "user-6", "34000", "merchant-F"),
                System.currentTimeMillis());

        // No alert yet (total = 69_000, threshold = 70_000)
        assertThat(harness.extractOutputValues()).isEmpty();

        // Advance processing time past the rolling window to fire the cleanup timer
        harness.setProcessingTime(harness.getProcessingTime() + WINDOW_MS + 1);

        // State should be cleared. A fresh transaction below threshold produces no alert.
        harness.processElement(transaction("tx-22", "user-6", "35000", "merchant-F"),
                System.currentTimeMillis());

        // Only one alert expected: the single large transaction after window reset
        // (35_000 < 50_000 large threshold, and rolling total restarted at 0 → 35_000 < 70_000)
        assertThat(harness.extractOutputValues()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Alert field integrity
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("FraudAlert preserves all fields from the originating transaction")
    void alert_preservesAllTransactionFields() throws Exception {
        long eventTs = 1_700_000_000_000L;
        TransactionEvent tx = new TransactionEvent("tx-99", "user-7",
                new BigDecimal("60000"), "merchant-G", eventTs);

        harness.processElement(tx, eventTs);

        FraudAlert alert = harness.extractOutputValues().get(0);
        assertThat(alert.getTransactionId()).isEqualTo("tx-99");
        assertThat(alert.getUserId()).isEqualTo("user-7");
        assertThat(alert.getAmount()).isEqualByComparingTo("60000");
        assertThat(alert.getMerchantId()).isEqualTo("merchant-G");
        assertThat(alert.getEventTimestamp()).isEqualTo(eventTs);     // from event, not wall clock
        assertThat(alert.getDetectedAt()).isGreaterThan(0);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static TransactionEvent transaction(String txId, String userId,
                                                String amount, String merchantId) {
        return new TransactionEvent(txId, userId, new BigDecimal(amount),
                merchantId, System.currentTimeMillis());
    }
}
