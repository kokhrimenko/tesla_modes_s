package com.kokhrimenko.tesla.model_s.state.impl;

import java.util.Comparator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.model.PageViewEvent;
import com.kokhrimenko.tesla.model_s.state.PageViewStore;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("!production")
public class InMemoryPageViewStore implements PageViewStore {
	private final ConcurrentHashMap<Integer, PriorityBlockingQueue<PageViewEvent>> pendingPageViews = new ConcurrentHashMap<>();

	@Override
	public Queue<PageViewEvent> getByPartition(int partition) {
		return pendingPageViews.get(partition);
	}

	@Override
	public void addPageView(PageViewEvent pageViewEvent) {
		pendingPageViews
				.computeIfAbsent(pageViewEvent.getPartition(),
						page -> new PriorityBlockingQueue<>(11, Comparator.comparing(PageViewEvent::getEventTime)))
				.add(pageViewEvent);
	}

	@Override
	public Set<Integer> getAllPartitions() {
		return pendingPageViews.keySet();
	}
}
