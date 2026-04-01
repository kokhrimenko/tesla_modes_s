package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Profile("production")
@Repository
@RequiredArgsConstructor
/**
 * WatermarkTracker using Redis. Ensures watermarks survive application crashes and restarts.
 */
public class RedisClickStateStore implements ClickStateStore {

    private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private String getKey(String userId) {
        return "clicks:user:" + userId;
    }
    
    public void addClick(AdClickEvent click) {
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
        String key = getKey(click.getUserId());
        try {
            String json = objectMapper.writeValueAsString(click);
            
            double score = click.getEventTime().toEpochMilli();
            
            redisTemplate.opsForZSet().add(key, json, score);

            redisTemplate.expire(key, ATTRIBUTION_WINDOW);
            
            log.debug("Saved click {} for user {} in Redis cache", click.getClickId(), click.getUserId());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize AdClickEvent", e);
            throw new RuntimeException("Serialization error", e);
        }
    }

    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);

        String key = getKey(userId);
        
        double maxScore = pageViewTime.toEpochMilli();
        double minScore = pageViewTime.minus(Duration.ofMinutes(30)).toEpochMilli();

		Set<String> results = redisTemplate.opsForZSet().reverseRangeByScore(key, minScore, maxScore, 0, 1);

        if (results == null || results.isEmpty()) {
            return null;
        }

        try {
            return objectMapper.readValue(results.iterator().next(), AdClickEvent.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize AdClickEvent", e);
            throw new RuntimeException("Serialization error", e);
        }
    }

    public int evictOldClicks(Instant cutoffTime) {
        log.debug("Evicting clicks older than {}", cutoffTime);

    	log.warn("Redis handle this");
        return 0;
    }

    public long getTotalClickCount() {
    	log.warn("Not Implemented yet. Redis handle key deletions, so it a bit more difficult");
        return 0;
    }
}
