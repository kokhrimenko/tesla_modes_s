package com.kokhrimenko.tesla.model_s.output.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kokhrimenko.tesla.model_s.model.AttributedPageView;
import com.kokhrimenko.tesla.model_s.output.OutputSink;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutputSinkLocalFile implements OutputSink {
	private final ObjectMapper objectMapper;

    @Value("${output.database.path:./output/attributed_page_views.db}")
    private String outputFilePath;
    private Writer fileWriter;

    @PostConstruct
    public void init() {
        try {
            // true flag enables append mode, so we don't overwrite on restart
        	OutputStreamWriter fw = new FileWriter(outputFilePath, true);
            this.fileWriter = new PrintWriter(new BufferedWriter(fw));
            log.info("OutputSink initialized. Writing attributed events to: {}", outputFilePath);
        } catch (IOException e) {
            log.error("Failed to initialize file writer for path: {}", outputFilePath, e);
            throw new RuntimeException("Could not initialize OutputSink", e);
        }
    }
    
	@Override
	public void write(AttributedPageView record) {
		try {
            // Convert the record to JSON string matching the required Output Schema
            String jsonOutput = objectMapper.writeValueAsString(record);
            
            // 1. Write to the local file
            fileWriter.write(jsonOutput);
            fileWriter.flush(); // Flush immediately to ensure no data is lost if the app crashes
            
            // 2. Also log it to the console for easy debugging/visibility
            log.info("EMITTED ATTRIBUTION: {}", jsonOutput);
            
        } catch (JsonProcessingException e) {
			log.error("Failed to serialize AttributedPageView to JSON. Record ID: {}", record.getPageViewId(), e);
        } catch (IOException e) {
        	log.error("Failed to write record to sink", e);

        	throw new RuntimeException("Could not write to sink", e);
		}
		
	}

}
