package com.kokhrimenko.tesla.model_s.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.model.AttributedPageView;
import com.kokhrimenko.tesla.model_s.model.PageViewEvent;
import com.kokhrimenko.tesla.model_s.output.OutputSink;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;
import com.kokhrimenko.tesla.model_s.state.PageViewStore;
import com.kokhrimenko.tesla.model_s.state.WatermarkTracker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 *
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 *
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {
	static final int IDLE_TOPICS_TRACKER = 60;
	private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final PageViewStore pageViewStore;
    private final OutputSink outputSink;
    
    private final AtomicReference<Instant> globalMaxEventTime = new AtomicReference<>(Instant.MIN);

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        log.debug("Processing click: {}", click.getClickId());

        if (watermarkTracker.isTooLate(click.getPartition(), click.getEventTime())) {
            log.warn("Dropped late ad click: {}", click.getClickId());
            //ToDo: introduce dead queue here
            return;
        }

        watermarkTracker.updateWatermark(click.getPartition(), click.getEventTime());
        updateGlobalMaxEventTime(click.getEventTime());

        clickStore.addClick(click);

        evaluatePendingPageViews(click.getPartition());
    }

	private void updateGlobalMaxEventTime(Instant eventTime) {
		globalMaxEventTime.accumulateAndGet(eventTime, (current, update) -> update.isAfter(current) ? update : current);
	}

	/**
     * Checks the buffer for the given partition and emits any page views 
     * that the watermark has successfully passed.
     */
    private void evaluatePendingPageViews(int partition) {
        Queue<PageViewEvent> queue = pageViewStore.getByPartition(partition);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        Instant safeTime = watermarkTracker.getWatermark(partition).minus(watermarkTracker.getAllowedLateness());

		synchronized (queue) {
			while (!queue.isEmpty()) {
				PageViewEvent oldest = queue.peek();
				if (oldest == null || oldest.getEventTime().equals(safeTime)
						|| oldest.getEventTime().isAfter(safeTime)) {
					break;
				}
				PageViewEvent readyToEmit = queue.poll();
				if (readyToEmit != null) {
					emitAttribution(readyToEmit);
				}
			}
		}
    }

    /**
     * Performs the actual state lookup and writes to the sink.
     */
    private void emitAttribution(PageViewEvent pageView) {
        AdClickEvent matchedClick = clickStore.findAttributableClick(pageView.getUserId(), pageView.getEventTime());

        AttributedPageView output = new AttributedPageView(
                pageView.getEventId(),
                pageView.getUserId(),
                pageView.getEventTime(),
                pageView.getUrl(),
                matchedClick != null ? matchedClick.getCampaignId() : null,
                matchedClick != null ? matchedClick.getClickId() : null
        );

        outputSink.write(output);
        log.debug("Emitted attributed page view: {} mapped to click: {}", output.getPageViewId(), output.getAttributedClickId());
    }
    
    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     *
     *
     * @param pageView the page view event
     */
    public void processPageView(PageViewEvent pageView) {
        log.debug("Processing page view: {}", pageView.getEventId());

        if (watermarkTracker.isTooLate(pageView.getPartition(), pageView.getEventTime())) {
            log.warn("Dropped late page view: {}", pageView.getEventId());
            return;
        }

        watermarkTracker.updateWatermark(pageView.getPartition(), pageView.getEventTime());
        updateGlobalMaxEventTime(pageView.getEventTime());

        pageViewStore.addPageView(pageView);

        // Check if this new page view (or any existing ones) are ready to be emitted
        evaluatePendingPageViews(pageView.getPartition());
    }

    /**
     * Scheduled task to evict old clicks from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldClicks() {
    	Instant currentMaxTime = globalMaxEventTime.get();
        if (currentMaxTime.equals(Instant.MIN)) {
            return;
        }

        Instant evictionCutoff = currentMaxTime
                .minus(watermarkTracker.getAllowedLateness())
                .minus(ATTRIBUTION_WINDOW);

        int evictedCount = clickStore.evictOldClicks(evictionCutoff);
        
		if (evictedCount > 0) {
			log.info("Evicted {} old clicks. Current total state size: {}", evictedCount, clickStore.getTotalClickCount());
		}
    }

    @Scheduled(fixedRate = IDLE_TOPICS_TRACKER, timeUnit = TimeUnit.SECONDS)
    protected void flushIdlePartitions() {
        Instant now = Instant.now();
		log.info("Flush indle messages at: {}", now);
        Duration idleThreshold = watermarkTracker.getAllowedLateness().plusSeconds(IDLE_TOPICS_TRACKER);

        for (Integer partition : pageViewStore.getAllPartitions()) {
            Instant lastActive = watermarkTracker.getWatermark(partition);

            if (Duration.between(lastActive, now).compareTo(idleThreshold) > 0) {
                log.debug("Partition {} is idle. Forcing watermark advancement.", partition);
                
                watermarkTracker.updateWatermark(partition, now);
                
                evaluatePendingPageViews(partition); 
            }
        }
    }
}
