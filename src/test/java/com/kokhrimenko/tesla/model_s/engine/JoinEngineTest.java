package com.kokhrimenko.tesla.model_s.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.model.AttributedPageView;
import com.kokhrimenko.tesla.model_s.model.PageViewEvent;
import com.kokhrimenko.tesla.model_s.output.OutputSink;
import com.kokhrimenko.tesla.model_s.state.impl.InMemoryClickStateStore;
import com.kokhrimenko.tesla.model_s.state.impl.InMemoryPageViewStore;
import com.kokhrimenko.tesla.model_s.state.impl.InMemoryWatermarkTracker;

@ExtendWith(MockitoExtension.class)
class JoinEngineTest {
	private static final int LATENESS_IN_MINS = 15;

	@Mock
	private InMemoryClickStateStore clickStore;
    @Mock
    private InMemoryWatermarkTracker watermarkTracker;
    @Mock
    private InMemoryPageViewStore pageViewStore;
    @Mock
    private OutputSink outputSink;

    @InjectMocks
    private JoinEngine joinEngine;

    private final Instant baseTime = Instant.now();

    @BeforeEach
    void setUp() {
        lenient().when(watermarkTracker.isTooLate(anyInt(), any())).thenReturn(false);
        lenient().when(watermarkTracker.getAllowedLateness()).thenReturn(Duration.ofMinutes(LATENESS_IN_MINS));
    }

    @Test
    @DisplayName("Click arrives first, then Page View")
    void testNormalAttribution() {
        final var partition = 0;
    	final var userId = "user_1";
    	final var pageView1 = "page_view_1";
    	final var campaign1 = "campaign_1";

		AdClickEvent click = createClick(campaign1, userId,
				baseTime.minus(LATENESS_IN_MINS + 3, ChronoUnit.MINUTES), campaign1, partition);
        when(watermarkTracker.getWatermark(partition)).thenReturn(baseTime);
        
        joinEngine.processClick(click);
        
		PageViewEvent pageViewEvent = createPageView(pageView1, userId,
				baseTime.minus(LATENESS_IN_MINS + 1, ChronoUnit.MINUTES), partition);
		when(pageViewStore.getByPartition(partition)).thenReturn(new ArrayDeque<>(List.of(pageViewEvent)));
        when(watermarkTracker.getWatermark(partition)).thenReturn(baseTime.plusSeconds(60));
        when(clickStore.findAttributableClick(userId, pageViewEvent.getEventTime())).thenReturn(click);

        joinEngine.processPageView(pageViewEvent);

		final var expectedAttributedPageView = new AttributedPageView(pageView1, userId, pageViewEvent.getEventTime(),
				pageViewEvent.getUrl(), campaign1, click.getClickId());
        verify(outputSink, times(1)).write(eq(expectedAttributedPageView));
    }

    @Test
    @DisplayName("Out-of-Order - Page View arrives, then Click, then Watermark advances")
    void testOutOfOrderAttribution() {
    	final var partition = 0;
    	final var userId1 = "user_1";
    	final var campaign1 = "campaign_1";
    	final var clickId1 = "click_1";
    	final var clickId2 = "click_2";
    	final int allowedLateness = 5;

    	when(watermarkTracker.getAllowedLateness()).thenReturn(Duration.ofSeconds(allowedLateness));
    	
        PageViewEvent pageViewEvent = createPageView("pv1", userId1, baseTime.plus(Duration.ofSeconds(1)), partition);
        when(pageViewStore.getByPartition(partition)).thenReturn(new ArrayDeque<>(List.of(pageViewEvent)));
        when(watermarkTracker.getWatermark(partition)).thenReturn(baseTime);
        joinEngine.processPageView(pageViewEvent);
        
        //page view came after watermark, no output
        verify(outputSink, never()).write(any());

        AdClickEvent click = createClick(clickId1, userId1, baseTime.plus(Duration.ofSeconds(2)), campaign1, partition);
        joinEngine.processClick(click);
        verify(clickStore).addClick(click);

        when(watermarkTracker.getWatermark(partition)).thenReturn(baseTime.plus(Duration.ofSeconds(allowedLateness + 2)));
        when(clickStore.findAttributableClick(eq(userId1), any())).thenReturn(click);

        joinEngine.processClick(createClick(clickId2, "user_2", baseTime, campaign1, partition));

        ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
        verify(outputSink).write(captor.capture());
        assertThat(captor.getValue().getAttributedClickId()).isEqualTo(clickId1);
    }

    @Test
    @DisplayName("Late Data - Event is older than watermark lateness and should be dropped")
    void testLateDataDropping() {
    	final var partition = 0;

        PageViewEvent latePageViewEvent = createPageView("pv_late", "user_1", baseTime, partition);
        when(watermarkTracker.isTooLate(partition, latePageViewEvent.getEventTime())).thenReturn(true);

        joinEngine.processPageView(latePageViewEvent);

        verify(outputSink, never()).write(any());
        verify(clickStore, never()).addClick(any());
    }

    @Test
    @DisplayName("No Click - Page View emitted with null attribution when no click found")
    void testNoMatchAttribution() {
    	final var partition = 0;
    	final var userId1 = "user_1";
    	final var pageView1 = "pv1";

        PageViewEvent pageViewEvent = createPageView(pageView1, userId1, baseTime.minus(LATENESS_IN_MINS + 1, ChronoUnit.MINUTES), partition);
        when(watermarkTracker.getWatermark(partition)).thenReturn(baseTime);
        when(clickStore.findAttributableClick(any(), any())).thenReturn(null);
		when(pageViewStore.getByPartition(partition)).thenReturn(new ArrayDeque<>(List.of(pageViewEvent)));
        joinEngine.processPageView(pageViewEvent);

        ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
        verify(outputSink).write(captor.capture());
        assertThat(captor.getValue().getAttributedClickId()).isNull();
        assertEquals(userId1, captor.getValue().getUserId());
        assertEquals(pageView1, captor.getValue().getPageViewId());
    }

    private AdClickEvent createClick(String id, String user, Instant time, String campaignId, int partition) {
        AdClickEvent click = new AdClickEvent();
        click.setClickId(id);
        click.setUserId(user);
        click.setEventTime(time);
        click.setPartition(partition);
        click.setCampaignId(campaignId);
        return click;
    }

    private PageViewEvent createPageView(String id, String user, Instant time, int partition) {
        PageViewEvent pv = new PageViewEvent();
        pv.setEventId(id);
        pv.setUserId(user);
        pv.setEventTime(time);
        pv.setPartition(partition);
        pv.setUrl("http://test.com");
        return pv;
    }
}
