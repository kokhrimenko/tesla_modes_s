package com.kokhrimenko.tesla.model_s.state.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@Profile("!production")
public class InMemoryClickStateStore implements ClickStateStore {

    private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final Map<String, UserClickState> store = new ConcurrentHashMap<>();

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
    
    public void addClick(AdClickEvent click) {
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
        UserClickState state = store.computeIfAbsent(click.getUserId(), k -> new UserClickState());
        
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

    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);

        UserClickState state = store.get(userId);
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

    public int evictOldClicks(Instant cutoffTime) {
        log.debug("Evicting clicks older than {}", cutoffTime);

        int totalEvicted = 0;
        Iterator<Map.Entry<String, UserClickState>> iterator = store.entrySet().iterator();
        
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
