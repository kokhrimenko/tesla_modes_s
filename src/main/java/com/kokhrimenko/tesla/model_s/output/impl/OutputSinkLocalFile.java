package com.kokhrimenko.tesla.model_s.output.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
			final var outputFile = new File(outputFilePath);

			if (!outputFile.getParentFile().exists()) {
				if (!outputFile.getParentFile().mkdir()) {
					throw new RuntimeException("Cannot create output sink directory");
				}
			}
			OutputStreamWriter fw = new FileWriter(outputFile, true);
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
            String jsonOutput = objectMapper.writeValueAsString(record);
            
            fileWriter.write(jsonOutput);
            fileWriter.flush();

            log.info("EMITTED ATTRIBUTION: {}", jsonOutput);
        } catch (IOException e) {
        	log.error("Failed to write record to sink", e);
        	throw new RuntimeException("Could not write to sink", e);
		}
	}
}
