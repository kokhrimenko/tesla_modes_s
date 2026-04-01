package com.kokhrimenko.tesla.model_s.state.impl;

import java.util.AbstractQueue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
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
	private final StringRedisTemplate redisTemplate;
	private final ObjectMapper objectMapper;
	private static final String KEY_PREFIX = "page_view_buffer:partition:";

	@Override
	public Queue<PageViewEvent> getByPartition(int partition) {
		return new RedisBackedPartitionQueue(partition);
	}

	@Override
	public void addPageView(PageViewEvent pageViewEvent) {
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

	private class RedisBackedPartitionQueue extends AbstractQueue<PageViewEvent> {
		private final String redisKey;

		public RedisBackedPartitionQueue(int partition) {
			this.redisKey = KEY_PREFIX + partition;
		}

		@Override
		public boolean isEmpty() {
			Long size = redisTemplate.opsForZSet().zCard(redisKey);
			return size == null || size == 0;
		}

		@Override
		public PageViewEvent peek() {
			Set<String> range = redisTemplate.opsForZSet().range(redisKey, 0, 0);
			if (range == null || range.isEmpty()) {
				return null;
			}
			return deserialize(range.iterator().next());
		}

		@Override
		public PageViewEvent poll() {
			TypedTuple<String> tuple = redisTemplate.opsForZSet().popMin(redisKey);
			if (tuple == null || tuple.getValue() == null) {
				return null;
			}
			return deserialize(tuple.getValue());
		}

		@Override
		public int size() {
			Long size = redisTemplate.opsForZSet().zCard(redisKey);
			return size != null ? size.intValue() : 0;
		}

		@Override
		public boolean offer(PageViewEvent pageViewEvent) {
			addPageView(pageViewEvent);
			return true;
		}

		@Override
		public Iterator<PageViewEvent> iterator() {
			throw new UnsupportedOperationException(
					"Iteration over Redis queue is not supported in this implementation");
		}

		private PageViewEvent deserialize(String json) {
			try {
				return objectMapper.readValue(json, PageViewEvent.class);
			} catch (JsonProcessingException e) {
				log.error("Deserialization error", e);
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public Set<Integer> getAllPartitions() {
		return redisTemplate.execute((RedisConnection connection) -> {
	        Set<Integer> keys = new HashSet<>();
	        
	        ScanOptions options = ScanOptions.scanOptions()
	                .match(KEY_PREFIX + "*")
	                .count(100)
	                .build();

			try (Cursor<byte[]> cursor = connection.keyCommands().scan(options)) {
				while (cursor.hasNext()) {
					String fullKey = new String(cursor.next());
					String partitionStr = fullKey.substring(KEY_PREFIX.length());
					keys.add(Integer.parseInt(partitionStr));
				}
			} catch (Exception e) {
				log.error("Failed to scan for buffer keys", e);
				throw new RuntimeException("Redis scan failed", e);
			}
	        
	        return keys;
	    });
	}
}
