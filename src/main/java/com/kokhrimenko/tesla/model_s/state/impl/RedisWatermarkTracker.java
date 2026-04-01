package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.state.WatermarkTracker;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("production")
public class RedisWatermarkTracker implements WatermarkTracker {
	private static final String PARTITION_WATERMARKS_KEY = "watermarks:partitions";

	private final Map<Integer, Instant> partitionWatermarks = new ConcurrentHashMap<>();
	
    private final Duration allowedLateness;
    private final StringRedisTemplate redisTemplate;

	public RedisWatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes,
			StringRedisTemplate redisTemplate) {
		this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
		this.redisTemplate = redisTemplate;
		log.info("Initialize WatermarkTracker with allowed lateness: `{}` minutes", allowedLatenessMinutes);
	}

	@PostConstruct
    public void initializeLocalCacheFromRedis() {
        log.info("Bootstrapping watermark cache from Redis...");
        
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(PARTITION_WATERMARKS_KEY);
        
        int loadedCount = 0;
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            try {
                int partition = Integer.parseInt(entry.getKey().toString());
                long timestamp = Long.parseLong(entry.getValue().toString());
                Instant watermarkTime = Instant.ofEpochMilli(timestamp);
                
                partitionWatermarks.put(partition, watermarkTime);
                loadedCount++;
                log.debug("Loaded watermark for partition `{}`, watermarkTime `{}`", partition, watermarkTime);
            } catch (NumberFormatException e) {
                log.error("Failed to parse watermark data from Redis for key {}", entry.getKey(), e);
            }
        }
        
        log.info("Successfully loaded {} partition watermarks from Redis.", loadedCount);
    }
	
	public void updateWatermark(int partition, Instant eventTime) {
		if (updateLocalCache(partition, eventTime)) {
			updateRedis(partition, eventTime);
		}
	}

	private boolean updateLocalCache(int partition, Instant eventTime) {
		Instant result = partitionWatermarks.compute(partition, (key, currentWatermark) -> {
            if (currentWatermark == null || eventTime.isAfter(currentWatermark)) {
                log.debug("Updating watermark for partition `{}` to `{}`", partition, eventTime);
                return eventTime;
            }
            return currentWatermark;
        });

		return result.equals(eventTime);
	}

	private void updateRedis(int partition, Instant eventTime) {
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
