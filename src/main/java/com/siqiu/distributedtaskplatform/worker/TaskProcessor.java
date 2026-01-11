package com.siqiu.distributedtaskplatform.worker;

public interface TaskProcessor {
    void process(long taskId);
}
