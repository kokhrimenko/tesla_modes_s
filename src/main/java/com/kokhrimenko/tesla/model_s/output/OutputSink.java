package com.kokhrimenko.tesla.model_s.output;

import org.springframework.stereotype.Component;

import com.kokhrimenko.tesla.model_s.model.AttributedPageView;

/**
 * Thread-safe sink that writes the final attributed page views to a local file
 * or standard out. In a fully distributed production environment, this could be
 * Kafka Producer (topic: `attributed_page_views`) or a JDBC batch writer to
 * push to a database.
 */
@Component
public interface OutputSink {

	void write(AttributedPageView record);
	
}
