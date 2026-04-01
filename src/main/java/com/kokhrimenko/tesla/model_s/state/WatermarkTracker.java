package com.kokhrimenko.tesla.model_s.state;

import java.time.Duration;
import java.time.Instant;

import org.springframework.stereotype.Repository;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 *
 * Watermark represents the point in event-time up to which we believe we have seen all events.
 * Events arriving with event_time < watermark - allowedLateness are considered too late.
 *
 */
@Repository
public interface WatermarkTracker {

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     */
    public void updateWatermark(int partition, Instant eventTime);

    /**
     * Get current watermark for a partition.
     *
     * @param partition the partition ID
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition);

    /**
     * Check if an event is too late (beyond allowed lateness).
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime);

    /**
     * Get the allowed lateness duration.
     *
     * @return the allowed lateness duration
     */
    public Duration getAllowedLateness();
}
