package com.kokhrimenko.tesla.model_s.state.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;

class InMemoryClickStateStoreTest {
	private ClickStateStore store;
	private final Instant baseTime = Instant.now();

	@BeforeEach
	void setUp() {
		store = new InMemoryClickStateStore();
	}

	@Test
	@DisplayName("Should find the most recent click within the 30-minute window")
	void testFindAttributableClick_WindowLogic() {
		final var userId = "user_1";
		final var latestClick = "click_3";

		//click too old
		store.addClick(createClick("click_1", userId, baseTime.minusSeconds(31 * 60)));

		store.addClick(createClick("click_2", userId, baseTime.minusSeconds(25 * 60)));
		store.addClick(createClick(latestClick, userId, baseTime.minusSeconds(5 * 60)));

		//click in the future
		store.addClick(createClick("click_4", userId, baseTime.plusSeconds(60)));

		AdClickEvent result = store.findAttributableClick(userId, baseTime);

		assertThat(result).isNotNull();
		assertThat(result.getClickId()).isEqualTo(latestClick);
	}

	@Test
	@DisplayName("Should return null for clicks older than 30 minutes exist")
	void testFindAttributableClick_Expired() {
		String userId = "user_1";
		store.addClick(createClick("click_1", userId, baseTime.minusSeconds(30 * 60 + 1)));

		AdClickEvent result = store.findAttributableClick(userId, baseTime);

		assertThat(result).isNull();
	}

	@Test
	@DisplayName("Should evict old clicks and remove empty users from the storage")
	void testEviction() {
		final var userId = "user_1";
		final var campaignId = "new";

		store.addClick(createClick("old", userId, baseTime.minusSeconds(40 * 60)));
		store.addClick(createClick(campaignId, userId, baseTime.minusSeconds(10 * 60)));

		int evicted = store.evictOldClicks(baseTime.minusSeconds(20 * 60));

		assertThat(evicted).isEqualTo(1);
		assertThat(store.getTotalClickCount()).isEqualTo(1);
		assertThat(store.findAttributableClick(userId, baseTime).getClickId()).isEqualTo(campaignId);
	}

	@Test
	@DisplayName("Should handle concurrent additions for the same user")
	void testConcurrency() throws InterruptedException {
		int threads = 10;
		int clicksPerThread = 100;
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		CountDownLatch latch = new CountDownLatch(threads);

		for (int i = 0; i < threads; i++) {
			final int threadIdx = i;
			executor.submit(() -> {
				try {
					for (int j = 0; j < clicksPerThread; j++) {
						store.addClick(createClick("t" + threadIdx + "c" + j, "sharedUser", baseTime));
					}
				} finally {
					latch.countDown();
				}
			});
		}

		latch.await(5, TimeUnit.SECONDS);
		executor.shutdown();

		assertThat(store.getTotalClickCount()).isEqualTo(threads * clicksPerThread);
	}

	@Test
	@DisplayName("Should handle duplicate clicks gracefully (idempotency)")
	void testDuplicateClicks() {
		AdClickEvent click = createClick("campaign_1", "user_1", baseTime);

		store.addClick(click);
		store.addClick(click);

		assertThat(store.getTotalClickCount()).isEqualTo(1);
	}

	private AdClickEvent createClick(String id, String userId, Instant time) {
		AdClickEvent click = new AdClickEvent();
		click.setClickId(id);
		click.setUserId(userId);
		click.setEventTime(time);
		click.setCampaignId("campaign_A");
		return click;
	}

}
