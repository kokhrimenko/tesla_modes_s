package com.kokhrimenko.tesla.model_s.consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kokhrimenko.tesla.model_s.engine.JoinEngine;
import com.kokhrimenko.tesla.model_s.model.AdClickEvent;
import com.kokhrimenko.tesla.model_s.model.PageViewEvent;

@ExtendWith(MockitoExtension.class)
class StreamConsumerTest {
	@Mock
    private JoinEngine joinEngine;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private StreamConsumer streamConsumer;

    private final String adClickJson = "{\"user_id\":\"u1\", \"click_id\":\"c1\"}";
    private final String pageViewJson = "{\"user_id\":\"u1\", \"event_id\":\"pv1\"}";

    @Test
    @DisplayName("Should process AdClick, enrich with metadata, and acknowledge offset")
    void consumeAdClick_Success() throws JsonMappingException, JsonProcessingException {
        // Arrange
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ad_clicks", 1, 100L, "key", adClickJson);
        AdClickEvent mockEvent = new AdClickEvent();
        when(objectMapper.readValue(adClickJson, AdClickEvent.class)).thenReturn(mockEvent);

        // Act
        streamConsumer.consumeAdClick(record, acknowledgment);

        // Assert
        verify(joinEngine).processClick(argThat(click -> 
            click.getPartition() == 1 && click.getOffset() == 100L
        ));
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should process PageView, enrich with metadata, and acknowledge offset")
    void consumePageView_Success() throws JsonProcessingException {
        // Arrange
        ConsumerRecord<String, String> record = new ConsumerRecord<>("page_views", 2, 500L, "key", pageViewJson);
        PageViewEvent mockEvent = new PageViewEvent();
        when(objectMapper.readValue(pageViewJson, PageViewEvent.class)).thenReturn(mockEvent);

        // Act
        streamConsumer.consumePageView(record, acknowledgment);

        // Assert
        verify(joinEngine).processPageView(argThat(pv -> 
            pv.getPartition() == 2 && pv.getOffset() == 500L
        ));
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should NOT acknowledge and should throw exception when AdClick processing fails")
    void consumeAdClick_Failure() throws JsonProcessingException {
        // Arrange
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ad_clicks", 1, 101L, "key", adClickJson);
        when(objectMapper.readValue(anyString(), eq(AdClickEvent.class))).thenReturn(new AdClickEvent());
        doThrow(new RuntimeException("Engine Failure")).when(joinEngine).processClick(any());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> streamConsumer.consumeAdClick(record, acknowledgment));
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("Should handle JSON deserialization errors gracefully by throwing RuntimeException")
    void consumePageView_DeserializationFailure() throws JsonProcessingException {
        // Arrange
        ConsumerRecord<String, String> record = new ConsumerRecord<>("page_views", 1, 102L, "key", "invalid-json");
        when(objectMapper.readValue(anyString(), eq(PageViewEvent.class)))
                .thenThrow(new JsonProcessingException("Parsing error") {});

        // Act & Assert
        assertThrows(RuntimeException.class, () -> streamConsumer.consumePageView(record, acknowledgment));
        verifyNoInteractions(joinEngine);
        verify(acknowledgment, never()).acknowledge();
    }
}
