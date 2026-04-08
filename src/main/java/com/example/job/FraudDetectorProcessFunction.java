package com.example.job;

import com.example.dto.FraudAlert;
import com.example.dto.FraudAlertSeverity;
import com.example.dto.FraudReason;
import com.example.dto.TransactionEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Core fraud-detection logic implemented as a Flink {@link KeyedProcessFunction}.
 *
 * <h3>Detection rules</h3>
 * <ol>
 *   <li><b>LARGE_TRANSACTION</b> — a single transaction {@code amount ≥ largeTransactionThreshold}.
 *       Severity is {@code CRITICAL} when amount ≥ 2× threshold, {@code HIGH} otherwise.</li>
 *   <li><b>ROLLING_SPEND_THRESHOLD_EXCEEDED</b> — a user's cumulative spend within the
 *       current rolling window ≥ rapidTotalThreshold. Severity is {@code MEDIUM}.</li>
 * </ol>
 *
 * <h3>State management</h3>
 * <p>Per-user rolling spend is held in {@link ValueState} keyed by {@code userId}.
 * On the first transaction in a window a processing-time timer is registered for
 * {@code currentTime + rollingWindowMs}. When that timer fires, the state is cleared
 * so the next window starts fresh. Without this reset the rolling total would grow
 * unboundedly and trigger spurious alerts indefinitely.
 *
 * <h3>Monetary arithmetic</h3>
 * <p>{@link BigDecimal} is used throughout to avoid the floating-point precision
 * loss inherent in {@code double} — critical for financial thresholds.
 * Note: Flink serialises {@code BigDecimal} state via Kryo. For very high-throughput
 * production use-cases, consider a custom {@code TypeSerializer<BigDecimal>}.
 */
public class FraudDetectorProcessFunction
        extends KeyedProcessFunction<String, TransactionEvent, FraudAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectorProcessFunction.class);

    private final BigDecimal largeTransactionThreshold;
    private final BigDecimal rapidTotalThreshold;
    private final long       rollingWindowMs;

    private transient ValueState<BigDecimal> rollingSpendState;

    public FraudDetectorProcessFunction(BigDecimal largeTransactionThreshold,
                                        BigDecimal rapidTotalThreshold,
                                        long rollingWindowMs) {
        this.largeTransactionThreshold = largeTransactionThreshold;
        this.rapidTotalThreshold       = rapidTotalThreshold;
        this.rollingWindowMs           = rollingWindowMs;
    }

    @Override
    public void open(Configuration parameters) {
        rollingSpendState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("rolling-spend", BigDecimal.class));
    }

    @Override
    public void processElement(TransactionEvent tx,
                               Context ctx,
                               Collector<FraudAlert> out) throws Exception {
        BigDecimal currentTotal = rollingSpendState.value();

        if (currentTotal == null) {
            // First transaction in this rolling window — register a cleanup timer.
            currentTotal = BigDecimal.ZERO;
            long windowEnd = ctx.timerService().currentProcessingTime() + rollingWindowMs;
            ctx.timerService().registerProcessingTimeTimer(windowEnd);
            LOG.debug("New rolling window opened for user={} expiresAt={}", ctx.getCurrentKey(), windowEnd);
        }

        BigDecimal updatedTotal = currentTotal.add(tx.getAmount());
        rollingSpendState.update(updatedTotal);

        LOG.debug("user={} rollingSpend={} txAmount={}", ctx.getCurrentKey(), updatedTotal, tx.getAmount());

        // Rule 1: single large transaction
        if (tx.getAmount().compareTo(largeTransactionThreshold) >= 0) {
            FraudAlertSeverity severity = tx.getAmount()
                    .compareTo(largeTransactionThreshold.multiply(BigDecimal.valueOf(2))) >= 0
                    ? FraudAlertSeverity.CRITICAL
                    : FraudAlertSeverity.HIGH;
            out.collect(buildAlert(tx, FraudReason.LARGE_TRANSACTION, severity));

        // Rule 2: rolling spend threshold breached
        } else if (updatedTotal.compareTo(rapidTotalThreshold) >= 0) {
            out.collect(buildAlert(tx, FraudReason.ROLLING_SPEND_THRESHOLD_EXCEEDED, FraudAlertSeverity.MEDIUM));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAlert> out) throws Exception {
        // Rolling window has expired — reset so the next window starts from zero.
        LOG.info("Rolling window expired for user={}, resetting spend state", ctx.getCurrentKey());
        rollingSpendState.clear();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private FraudAlert buildAlert(TransactionEvent tx, FraudReason reason, FraudAlertSeverity severity) {
        return new FraudAlert(
                tx.getTransactionId(),
                tx.getUserId(),
                tx.getAmount(),
                tx.getMerchantId(),       // preserved — important for card-testing detection
                reason,
                severity,
                tx.getEventTimestamp(),   // from the event, NOT Instant.now()
                System.currentTimeMillis() // wall-clock detection time for latency tracking
        );
    }
}
