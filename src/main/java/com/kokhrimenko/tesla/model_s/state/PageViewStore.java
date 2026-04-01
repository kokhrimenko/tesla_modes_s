package com.kokhrimenko.tesla.model_s.state;

import java.util.Queue;

import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.model.PageViewEvent;

/**
 * Stores page view events.
 *
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 *
 */
@Repository
public interface PageViewStore {

	Queue<PageViewEvent> getByPartition(int partition);
	
	void addPageView(PageViewEvent pageViewEvent);
}
