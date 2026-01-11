package com.siqiu.distributedtaskplatform.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DefaultTaskProcessor implements TaskProcessor {
    private static final Logger log = LoggerFactory.getLogger(DefaultTaskProcessor.class);

    @Override
    public void process(long taskId) {
        // Replace with real logic later
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Processed taskId={}", taskId);
    }
}
