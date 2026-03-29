package com.kokhrimenko.tesla.model_s.state;

import java.time.Instant;

import org.springframework.stereotype.Component;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 *
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 *
 */
@Component
public interface ClickStateStore {    
    /**
     * Add a click event to the state store.
     *
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click);

    /**
     * Find the most recent click for a user within the attribution window.
     *
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime);

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     *
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime);

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount();
}
