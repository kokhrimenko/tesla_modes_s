package com.kokhrimenko.tesla.model_s;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

import lombok.extern.slf4j.Slf4j;

/**
 * Main application for the Real-time Session Attribution Stream Processor.
 *
 * This application performs windowed joins between page view and ad click streams
 * to attribute page views to ad campaigns in real-time.
 */
@Slf4j
@SpringBootApplication
@EnableKafka
public class StreamProcessorApplication {

    public static void main(String[] args) {
        log.info("Starting Tesla Mode_s task Application...");
        SpringApplication.run(StreamProcessorApplication.class, args);
        log.info("Tesla Mode_s task Application started successfully");
    }
}
