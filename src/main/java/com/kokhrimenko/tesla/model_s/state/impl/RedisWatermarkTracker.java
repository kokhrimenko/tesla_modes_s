package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.state.WatermarkTracker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("production")
public class RedisWatermarkTracker implements WatermarkTracker {
	private static final String PARTITION_WATERMARKS_KEY = "watermarks:partitions";

    private final Duration allowedLateness;
    private final StringRedisTemplate redisTemplate;

    public RedisWatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes, StringRedisTemplate redisTemplate) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        this.redisTemplate = redisTemplate;
        log.info("Initialize WatermarkTracker with allowed lateness: `{}` minutes", allowedLatenessMinutes);
    }

    public void updateWatermark(int partition, Instant eventTime) {
    	String hashKey = String.valueOf(partition);
        long newTimestamp = eventTime.toEpochMilli();

        Object currentVal = redisTemplate.opsForHash().get(PARTITION_WATERMARKS_KEY, hashKey);
        
        long currentTimestamp = currentVal != null ? Long.parseLong(currentVal.toString()) : 0L;

        if (newTimestamp > currentTimestamp) {
            redisTemplate.opsForHash().put(PARTITION_WATERMARKS_KEY, hashKey, String.valueOf(newTimestamp));
            log.debug("Updating watermark for partition `{}` to `{}`", partition, eventTime);
        }
    }

    private static Instant getDefaultWatermark() {
    	return Instant.MIN;
    }
    
    public Instant getWatermark(int partition) {
    	Object val = redisTemplate.opsForHash().get(PARTITION_WATERMARKS_KEY, String.valueOf(partition));
        if (val == null) {
            return getDefaultWatermark();
        }
        return Instant.ofEpochMilli(Long.parseLong(val.toString()));
    }

    public boolean isTooLate(int partition, Instant eventTime) {
    	Instant currentWatermark = getWatermark(partition);
        
        if (currentWatermark.equals(Instant.MIN)) {
            return false;
        }

        Instant dropThreshold = currentWatermark.minus(allowedLateness);
        return eventTime.isBefore(dropThreshold);
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
