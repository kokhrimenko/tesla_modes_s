package com.kokhrimenko.tesla.model_s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kokhrimenko.tesla.model_s.model.AttributedPageView;
import com.kokhrimenko.tesla.model_s.output.OutputSink;

@SpringBootTest
@Testcontainers
public class StreamProcessorIntegrationTest {
	private static final String AD_CLICK_TOPIC = "ad_clicks_test";
	private static final String PAGE_VIEWS_TOPIC = "page_views_test";

	@Container
	static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
			.withZone(ZoneId.systemDefault());
	
	@DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
		final var bootstrapServers = kafka.getBootstrapServers();

        registry.add("spring.kafka.bootstrap-servers", () -> bootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", () -> bootstrapServers);
        
        registry.add("kafka.bootstrap-servers", () -> bootstrapServers);
    }
	
	@TestConfiguration
	@EnableKafka
	static class KafkaTestConfig {
		@Bean
		public NewTopic adClicksTopic() {
			return new NewTopic(AD_CLICK_TOPIC, 3, (short) 1);
		}

		@Bean
		public NewTopic pageViewsTopic() {
			return new NewTopic(PAGE_VIEWS_TOPIC, 3, (short) 1);
		}

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}
	}
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@MockitoSpyBean
	private OutputSink outputSink;

	@BeforeEach
	void setup() {
		clearInvocations(outputSink);
	}

	@AfterAll
	public static void tearDown() {
		kafka.stop();
	}
	
	@Test
	@DisplayName("Should handle Out-of-Order events and attribute to the most recent click")
	void testOutOfOrderAndMultipleClicks() {
		final var now = Instant.now();
		final var userId1 = "user_" + UUID.randomUUID().toString();
		final var clickId1 = "click_" + UUID.randomUUID().toString();
		final var clickId2 = "click_" + UUID.randomUUID().toString();
		final var campaignId1 = "campaign_" + UUID.randomUUID().toString();
		final var campaignId2 = "campaign_" + UUID.randomUUID().toString();
		final var pageViewId = "page_view_" + UUID.randomUUID().toString();
		
		sendPageView(pageViewId, userId1, formatter.format(now.minus(5, ChronoUnit.MINUTES)));

		sendAdClick(clickId1, userId1, formatter.format(Instant.now().minus(15, ChronoUnit.MINUTES)), campaignId1);

		sendAdClick(clickId2, userId1, formatter.format(Instant.now().minus(8, ChronoUnit.MINUTES)), campaignId2);

		sendAdClick("dummy", "user_dummy", formatter.format(Instant.now()), "dummy_camp");

		await().atMost(Duration.ofSeconds(100)).untilAsserted(() -> verify(outputSink, atLeastOnce()).write(any()));

		ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
		verify(outputSink, atLeast(1)).write(captor.capture());

		List<AttributedPageView> outputs = captor.getAllValues();
		AttributedPageView targetOutput = outputs
				.stream()
				.filter(o -> pageViewId.equals(o.getPageViewId()))
				.findFirst()
				.orElseThrow();

		// Should be attributed to click_2 (camp_B) because it's the MOST RECENT within 30 minutes
		assertThat(targetOutput.getAttributedClickId()).isEqualTo(clickId2);
		assertThat(targetOutput.getAttributedCampaignId()).isEqualTo(campaignId2);
	}

	@Test
	@DisplayName("Old clicks (> 30 mins) should result in Null attribution")
	void testOldClickNoAttribution() {
		final var now = Instant.now();

		final var realUser = "user_" + UUID.randomUUID().toString();
		final var dummyUser = "user_dummy";
		final var oldClickId = "click_old";
		final var pageViewId = "page_view_" + UUID.randomUUID().toString();

		// Click at 11:30
		sendAdClick(oldClickId, realUser, formatter.format(now.minus(1, ChronoUnit.HOURS)), "camp_C");

		// Page View at 12:10 (> 30 mins later)
		sendPageView(pageViewId, realUser, formatter.format(now.minus(1, ChronoUnit.MINUTES)));

		// Advance watermark
		sendAdClick("dummy", dummyUser, formatter.format(now) , "dummy_camp");

		await().atMost(Duration.ofSeconds(100)).untilAsserted(() -> {
			ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
			verify(outputSink, atLeast(1)).write(captor.capture());

			boolean hasProcessedCorrectCampaign = captor.getAllValues()
					.stream()
					.anyMatch(o -> pageViewId.equals(o.getPageViewId()) && o.getAttributedClickId() == null);

			assertThat(hasProcessedCorrectCampaign).isTrue();
		});
	}

	private void sendAdClick(String clickId, String userId, String time, String campaignId) {
		String payload = String.format(
				"{\"user_id\":\"%s\", \"event_time\":\"%s\", \"campaign_id\":\"%s\", \"click_id\":\"%s\"}", userId,
				time, campaignId, clickId);
		kafkaTemplate.send(AD_CLICK_TOPIC, userId, payload);
	}

	private void sendPageView(String eventId, String userId, String time) {
		String payload = String.format(
				"{\"user_id\":\"%s\", \"event_time\":\"%s\", \"url\":\"http://test.com\", \"event_id\":\"%s\"}", userId,
				time, eventId);
		kafkaTemplate.send(PAGE_VIEWS_TOPIC, userId, payload);
	}
}
