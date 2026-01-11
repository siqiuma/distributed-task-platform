package com.siqiu.distributedtaskplatform.queue;

public interface DeadLetterClient {
    void publishDeadTask(DeadTaskEvent event);
}
