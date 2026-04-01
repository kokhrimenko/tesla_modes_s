package com.kokhrimenko.tesla.model_s.state.impl;

import java.util.Comparator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kokhrimenko.tesla.model_s.model.PageViewEvent;
import com.kokhrimenko.tesla.model_s.state.PageViewStore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("production")
@RequiredArgsConstructor
public class RedisPageViewStore implements PageViewStore {
	private final ConcurrentHashMap<Integer, PriorityBlockingQueue<PageViewEvent>> pendingPageViews = new ConcurrentHashMap<>();

	private final StringRedisTemplate redisTemplate;
	private final ObjectMapper objectMapper;
	private static final String KEY_PREFIX = "page_view_buffer:partition:";

	@Override
	public Queue<PageViewEvent> getByPartition(int partition) {
		return pendingPageViews.get(partition);
	}

	@Override
	public void addPageView(PageViewEvent pageViewEvent) {
		updateLocalCache(pageViewEvent);

		updateRedis(pageViewEvent);
	}

	private void updateLocalCache(PageViewEvent pageViewEvent) {
		pendingPageViews
				.computeIfAbsent(pageViewEvent.getPartition(),
						page -> new PriorityBlockingQueue<>(11, Comparator.comparing(PageViewEvent::getEventTime)))
				.add(pageViewEvent);
	}

	private void updateRedis(PageViewEvent pageViewEvent) {
		String key = KEY_PREFIX + pageViewEvent.getPartition();
		try {
			String json = objectMapper.writeValueAsString(pageViewEvent);
			double score = pageViewEvent.getEventTime().toEpochMilli();

			redisTemplate.opsForZSet().add(key, json, score);
		} catch (JsonProcessingException e) {
			log.error("Serialization error", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<Integer> getAllPartitions() {
		return pendingPageViews.keySet();
	}
}
