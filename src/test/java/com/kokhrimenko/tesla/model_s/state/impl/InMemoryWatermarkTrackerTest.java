package com.kokhrimenko.tesla.model_s.state.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kokhrimenko.tesla.model_s.state.WatermarkTracker;

public class InMemoryWatermarkTrackerTest {
	private static final int LATENESS_TIME = 15;

	private final Instant baseTime = Instant.now();

	private WatermarkTracker tracker;

	@BeforeEach
	void setUp() {
		tracker = new InMemoryWatermarkTracker(LATENESS_TIME);
	}

	@Test
	@DisplayName("Watermark should advance monotonically and never go backward")
	void testMonotonicity() {
		final int partition = 0;

		tracker.updateWatermark(partition, baseTime);
		assertThat(tracker.getWatermark(partition)).isEqualTo(baseTime);

		tracker.updateWatermark(partition, baseTime.minusSeconds(600));

		assertThat(tracker.getWatermark(partition)).isEqualTo(baseTime);
	}

	@Test
	@DisplayName("Should correctly identify late events based on allowed lateness")
	void testIsTooLate() {
		final int partition = 0;

		tracker.updateWatermark(partition, baseTime.plusSeconds(1200));

		assertThat(tracker.isTooLate(partition, baseTime.plusSeconds(600))).isFalse();

		assertThat(tracker.isTooLate(partition, baseTime.plusSeconds(240))).isTrue();
	}

	@Test
	@DisplayName("Should not drop events if watermark is not yet initialized")
	void testLatenessWithNoWatermark() {
		final int partition = 0;

		assertThat(tracker.isTooLate(partition, baseTime)).isFalse();
	}

	@Test
	@DisplayName("Should track watermarks independently for different partitions")
	void testPartitionIsolation() {
		int partition0 = 0;
		int partition1 = 1;

		tracker.updateWatermark(partition0, baseTime);
		tracker.updateWatermark(partition1, baseTime.plusSeconds(3600));

		assertThat(tracker.getWatermark(partition0)).isEqualTo(baseTime);
		assertThat(tracker.getWatermark(partition1)).isEqualTo(baseTime.plusSeconds(3600));
	}
}
