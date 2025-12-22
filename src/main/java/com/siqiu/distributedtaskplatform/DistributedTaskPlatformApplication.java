package com.siqiu.distributedtaskplatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling //Allow background jobs
public class DistributedTaskPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedTaskPlatformApplication.class, args);
    }

}
