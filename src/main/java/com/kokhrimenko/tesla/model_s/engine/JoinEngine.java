package com.kokhrimenko.tesla.model_s.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.model.AttributedPageView;
import com.kokhrimenko.tesla.model_s.model.PageViewEvent;
import com.kokhrimenko.tesla.model_s.output.OutputSink;
import com.kokhrimenko.tesla.model_s.state.ClickStateStore;
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
	private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;
    
    private final AtomicReference<Instant> globalMaxEventTime = new AtomicReference<>(Instant.MIN);
    private final ConcurrentHashMap<Integer, PriorityBlockingQueue<PageViewEvent>> pendingPageViews = new ConcurrentHashMap<>();

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     *
     * TODO: Implement click processing logic
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        log.debug("Processing click: {}", click.getClickId());

        watermarkTracker.updateWatermark(click.getPartition(), click.getEventTime());
        updateGlobalMaxEventTime(click.getEventTime());

        if (watermarkTracker.isTooLate(click.getPartition(), click.getEventTime())) {
            log.warn("Dropped late ad click: {}", click.getClickId());
            //ToDo: introduce dead queue here
            return;
        }

        clickStore.addClick(click);
        
        // Advancing the watermark might have unblocked some buffered page views
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
        PriorityBlockingQueue<PageViewEvent> queue = pendingPageViews.get(partition);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        Instant currentWatermark = watermarkTracker.getWatermark(partition);

        // Peek at the oldest page view in the queue. 
        // If its event time is <= the current watermark, we are confident no more 
        // non-late clicks will arrive before it. We can safely emit.
        while (!queue.isEmpty()) {
            PageViewEvent oldest = queue.peek();
            
            // If the oldest event is strictly AFTER the watermark, it's not ready yet.
            // Because the queue is sorted, none of the newer events are ready either.
            if (oldest == null || !currentWatermark.isAfter(oldest.getEventTime())) {
                break;
            }

            // It is ready! Remove it from the buffer and process it.
            PageViewEvent readyToEmit = queue.poll();
            if (readyToEmit != null) {
                emitAttribution(readyToEmit);
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
     * TODO: Implement page view processing logic
     * - Check if event is too late using watermarkTracker
     * - Find attributable click from clickStore
     * - Create and emit AttributedPageView
     * - Update watermark for the partition
     *
     * @param pageView the page view event
     */
    public void processPageView(PageViewEvent pageView) {
        log.debug("Processing page view: {}", pageView.getEventId());

        watermarkTracker.updateWatermark(pageView.getPartition(), pageView.getEventTime());
        updateGlobalMaxEventTime(pageView.getEventTime());

        if (watermarkTracker.isTooLate(pageView.getPartition(), pageView.getEventTime())) {
            log.warn("Dropped late page view: {}", pageView.getEventId());
            return;
        }

        // BUFFER the page view instead of emitting it immediately
        pendingPageViews.computeIfAbsent(pageView.getPartition(),
                p -> new PriorityBlockingQueue<>(11, Comparator.comparing(PageViewEvent::getEventTime))
        ).add(pageView);

        // Check if this new page view (or any existing ones) are ready to be emitted
        evaluatePendingPageViews(pageView.getPartition());
    }

    /**
     * Scheduled task to evict old clicks from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     * TODO: Implement state eviction logic
     * - Evict clicks older than the watermark cutoff
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
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
			log.info("Evicted {} old clicks. Current total state size: {}", evictedCount,
					clickStore.getTotalClickCount());
		}
    }
}
