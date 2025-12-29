package com.siqiu.distributedtaskplatform;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {DistributedTaskPlatformApplication.class, TestcontainersConfig.class})
@ActiveProfiles("test")
class PostgresContainerSmokeTest {

    @Test
    void contextLoads() {
        // If the app starts, Flyway ran and the schema exists
    }
}
