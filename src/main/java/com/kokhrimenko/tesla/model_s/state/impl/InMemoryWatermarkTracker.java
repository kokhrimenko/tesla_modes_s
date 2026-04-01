package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.state.WatermarkTracker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("!production")
public class InMemoryWatermarkTracker implements WatermarkTracker {

    private final Duration allowedLateness;
    
    private final Map<Integer, Instant> partitionWatermarks = new ConcurrentHashMap<>();

    public InMemoryWatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialize WatermarkTracker with allowed lateness: `{}` minutes", allowedLatenessMinutes);
    }

    public void updateWatermark(int partition, Instant eventTime) {
    	partitionWatermarks.compute(partition, (key, currentWatermark) -> {
            if (currentWatermark == null || eventTime.isAfter(currentWatermark)) {
                log.debug("Updating watermark for partition `{}` to `{}`", partition, eventTime);
                return eventTime;
            }
            return currentWatermark;
        });
    }

    private static Instant getDefaultWatermark() {
    	return Instant.MIN;
    }
    
    public Instant getWatermark(int partition) {
    	return partitionWatermarks.getOrDefault(partition, getDefaultWatermark());
    }

    public boolean isTooLate(int partition, Instant eventTime) {
    	Instant currentWatermark = getWatermark(partition);
        
        if (currentWatermark.equals(getDefaultWatermark())) {
            return false;
        }

        Instant cutoffTime = currentWatermark.minus(getAllowedLateness());
        
        return eventTime.isBefore(cutoffTime);
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
