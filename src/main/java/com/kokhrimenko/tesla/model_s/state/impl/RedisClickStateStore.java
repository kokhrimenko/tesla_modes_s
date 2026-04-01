package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Profile("production")
@Repository
@RequiredArgsConstructor
/**
 * WatermarkTracker using Redis and InMemory cache for higher performance.
 * Ensures watermarks survive application crashes and restarts. Cache will be
 * initialised from Redis on start-up.
 */
public class RedisClickStateStore implements ClickStateStore {

    private static final String REDIS_CLICKS_USER = "clicks:user:";

	private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private final Map<String, UserClickState> localStore = new ConcurrentHashMap<>();
    private final AtomicLong totalClicks = new AtomicLong(0);

   /**
    * Inner class representing the state for a single user.
    * Uses a ReentrantLock for fine-grained, per-user concurrency.
    */
	private static class UserClickState {
		final ReentrantLock lock = new ReentrantLock();

		boolean isDetached = false;

		final TreeSet<AdClickEvent> clicks = new TreeSet<>(
				Comparator.comparing(AdClickEvent::getEventTime).reversed().thenComparing(AdClickEvent::getClickId));
	}
    
    private String getKey(String userId) {
        return REDIS_CLICKS_USER + userId;
    }

    @PostConstruct
	public void initializeCacheFromRedis() {
		log.info("Starting local cache initialization from Redis...");
		int loadedUsers = 0;
		int loadedClicks = 0;
		long minValidTs = Instant.now().minus(ATTRIBUTION_WINDOW).toEpochMilli();

		ScanOptions options = ScanOptions.scanOptions().match(REDIS_CLICKS_USER + "*").count(500).build();

		try (Cursor<byte[]> cursor = redisTemplate.getConnectionFactory().getConnection().keyCommands().scan(options)) {
			while (cursor.hasNext()) {
				String key = new String(cursor.next());
				String userId = key.substring(REDIS_CLICKS_USER.length());

				Set<ZSetOperations.TypedTuple<String>> entries = redisTemplate.opsForZSet().rangeWithScores(key, 0, -1);

				if (entries != null && !entries.isEmpty()) {
					UserClickState userClicks = new UserClickState();

					for (ZSetOperations.TypedTuple<String> entry : entries) {
						if (entry.getScore() != null && entry.getScore() >= minValidTs) {
							try {
								AdClickEvent click = objectMapper.readValue(entry.getValue(), AdClickEvent.class);
								userClicks.clicks.add(click);
								loadedClicks++;
							} catch (JsonProcessingException e) {
								log.error("Failed to deserialize click for user {}", userId, e);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Failed to initialize local cache from Redis", e);
		}

		log.info("Finished cache initialization. Loaded {} active clicks across {} users.", loadedClicks, loadedUsers);
	}
    
    public void addClick(AdClickEvent click) {
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
        updateLocalCache(click);
        updateRedis(click);
    }

	private void updateLocalCache(AdClickEvent click) {
		UserClickState state = localStore.computeIfAbsent(click.getUserId(), k -> new UserClickState());
        
        while (true) {
        	state.lock.lock();
        	try {
        		if (state.isDetached) {
        			continue;
        		}
            
        		if (state.clicks.add(click)) {
        			totalClicks.incrementAndGet();
        		}
        		break;
        	} finally {
        		state.lock.unlock();
        	}
        }
	}

	private void updateRedis(AdClickEvent click) {
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

        UserClickState state = localStore.get(userId);
        if (state == null) {
        	log.debug("No clicks found for user {}", userId);
            return null;
        }

        Instant windowStart = pageViewTime.minus(ATTRIBUTION_WINDOW);
        
        state.lock.lock();
        try {
        	return state.clicks.stream()
        			.filter(click -> click.getEventTime().isBefore(pageViewTime))
        	        .filter(click -> click.getEventTime().isAfter(windowStart))
        			.findFirst()
        			.orElseGet(() -> null);
        } finally {
            state.lock.unlock();
        }
    }

    //No redis updates needed
    public int evictOldClicks(Instant cutoffTime) {
    	log.debug("Evicting clicks older than {}", cutoffTime);

        int totalEvicted = 0;
        Iterator<Map.Entry<String, UserClickState>> iterator = localStore.entrySet().iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<String, UserClickState> entry = iterator.next();
            UserClickState state = entry.getValue();

            int evictedForUser = 0;

            state.lock.lock();
            try {
                Iterator<AdClickEvent> clickIterator = state.clicks.reversed().iterator();
                while (clickIterator.hasNext()) {
                    AdClickEvent click = clickIterator.next();
                    if (click.getEventTime().isBefore(cutoffTime)) {
                        clickIterator.remove();
                        evictedForUser++;
                    } else {
                        break;
                    }
                }

                totalEvicted += evictedForUser;
                totalClicks.addAndGet(-evictedForUser);

                if (state.clicks.isEmpty()) {
                    state.isDetached = true;
                    iterator.remove();
                }
            } finally {
                state.lock.unlock();
            }
        }

        return totalEvicted;
    }

    public long getTotalClickCount() {
    	return totalClicks.get();
    }
}
