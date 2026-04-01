package com.kokhrimenko.tesla.model_s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	private static final int CONCURRENCY = 2;

	@SuppressWarnings("resource")
	@Container
	static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
			.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
			.withEnv("KAFKA_NUM_PARTITIONS", String.valueOf(CONCURRENCY));

	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
			.withZone(ZoneId.systemDefault());

	@DynamicPropertySource
	static void overrideProperties(DynamicPropertyRegistry registry) {
		final var bootstrapServers = kafka.getBootstrapServers();

		registry.add("kafka.topics.page-views", () -> PAGE_VIEWS_TOPIC);
		registry.add("kafka.topics.ad-clicks", () -> AD_CLICK_TOPIC);
		registry.add("kafka.consumer.group-id", () -> "stream-processor-group_test");
		registry.add("spring.kafka.bootstrap-servers", () -> bootstrapServers);
		registry.add("spring.kafka.consumer.bootstrap-servers", () -> bootstrapServers);
		registry.add("kafka.consumer.concurrency", () -> CONCURRENCY);

		registry.add("kafka.bootstrap-servers", () -> bootstrapServers);
	}

	@TestConfiguration
	@EnableKafka
	static class KafkaTestConfig {
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

	@Value("${watermark.allowed-lateness-minutes:15}")
	private int allowedLateness;

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
	void testOutOfOrderAndMultipleClicks() throws InterruptedException {
		final var partition = 0;
		final var now = Instant.now();
		final var userId1 = "user_tc1_" + UUID.randomUUID().toString();
		final var clickId1 = "click_tc1_" + UUID.randomUUID().toString();
		final var clickId2 = "click_tc1_" + UUID.randomUUID().toString();
		final var campaignId1 = "campaign_tc1_" + UUID.randomUUID().toString();
		final var campaignId2 = "campaign_tc1_" + UUID.randomUUID().toString();
		final var pageViewId = "page_view_tc1_" + UUID.randomUUID().toString();

		sendPageView(pageViewId, partition, userId1, formatter.format(now.minus(allowedLateness, ChronoUnit.MINUTES)));

		sendAdClick(clickId1, partition, userId1, formatter.format(now.minus(25, ChronoUnit.MINUTES)), campaignId1);

		sendAdClick(clickId2, partition, userId1, formatter.format(now.minus(22, ChronoUnit.MINUTES)), campaignId2);

		sendAdClick("dummy", partition, "user_dummy", formatter.format(now.plusSeconds(15)), "dummy_camp");

		ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
		await()
			.atMost(Duration.ofSeconds(80))
			.untilAsserted(() -> verify(outputSink, atLeastOnce()).write(captor.capture()));

		List<AttributedPageView> outputs = captor.getAllValues();
		AttributedPageView targetOutput = outputs.stream()
				.filter(o -> pageViewId.equals(o.getPageViewId()))
				.findFirst()
				.orElseThrow();

		// Should be attributed to click_2 (camp_B) because it's the MOST RECENT within
		// 30 minutes
		assertThat(targetOutput.getAttributedClickId()).isEqualTo(clickId2);
		assertThat(targetOutput.getAttributedCampaignId()).isEqualTo(campaignId2);
	}

	@Test
	@DisplayName("Old clicks (> 30 mins) should result in Null attribution")
	void testOldClickNoAttribution() {
		final var partition = 1;
		final var now = Instant.now();

		final var realUser = "user_tc2_" + UUID.randomUUID().toString();
		final var dummyUser = "user_dummy_tc2";
		final var oldClickId = "click_old_tc2";
		final var pageViewId = "page_view_tc2_" + UUID.randomUUID().toString();

		sendAdClick(oldClickId, partition, realUser, formatter.format(now.minus(30, ChronoUnit.HOURS).minusSeconds(5)), "camp_C");

		sendPageView(pageViewId, partition, realUser, formatter.format(now));

		sendAdClick("dummy", partition, dummyUser, formatter.format(now.plus(allowedLateness, ChronoUnit.MINUTES).plusSeconds(10l)), "dummy_camp");

		await()
			.atMost(Duration.ofSeconds(80))
			.untilAsserted(() -> {
				ArgumentCaptor<AttributedPageView> captor = ArgumentCaptor.forClass(AttributedPageView.class);
				verify(outputSink, atLeastOnce()).write(captor.capture());

				boolean hasProcessedCorrectCampaign = captor.getAllValues().stream()
						.anyMatch(o -> pageViewId.equals(o.getPageViewId()) && o.getAttributedClickId() == null);

				assertThat(hasProcessedCorrectCampaign).isTrue();
			});
	}

	private void sendAdClick(String clickId, int partition, String userId, String time, String campaignId) {
		String payload = String.format(
				"{\"user_id\":\"%s\", \"event_time\":\"%s\", \"campaign_id\":\"%s\", \"click_id\":\"%s\"}", userId,
				time, campaignId, clickId);
		kafkaTemplate.send(AD_CLICK_TOPIC, partition, userId, payload);
	}

	private void sendPageView(String eventId, int partition, String userId, String time) {
		String payload = String.format(
				"{\"user_id\":\"%s\", \"event_time\":\"%s\", \"url\":\"http://test.com\", \"event_id\":\"%s\"}", userId,
				time, eventId);
		kafkaTemplate.send(PAGE_VIEWS_TOPIC, partition, userId, payload);
	}
}
