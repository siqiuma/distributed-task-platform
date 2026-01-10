package com.siqiu.distributedtaskplatform.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siqiu.distributedtaskplatform.DistributedTaskPlatformApplication;
import com.siqiu.distributedtaskplatform.TestcontainersConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.*;
import static org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace.NONE;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(classes = {DistributedTaskPlatformApplication.class, TestcontainersConfig.class}, properties = {
        "dtp.queue.mode=db",
        "spring.task.scheduling.enabled=false"
})
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = NONE)  // IMPORTANT: do not switch to H2
@Import(TestcontainersConfig.class)
@ActiveProfiles("test")
class TaskApiIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper om;

    @Test
    void createTask_thenGetTask_returnsTask() throws Exception {
        String body = """
            {"type":"email","payload":"hello"}
        """;

        String createdJson = mvc.perform(post("/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNumber())
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andReturn()
                .getResponse()
                .getContentAsString();

        long id = om.readTree(createdJson).get("id").asLong();

        mvc.perform(get("/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is((int) id)))
                .andExpect(jsonPath("$.type", is("email")))
                .andExpect(jsonPath("$.payload", is("hello")));
    }

    @Test
    void cancelTask_twice_secondReturns409() throws Exception {
        String body = """
            {"type":"email","payload":"hello"}
        """;

        String createdJson = mvc.perform(post("/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

        long id = om.readTree(createdJson).get("id").asLong();

        mvc.perform(put("/tasks/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELED")));

        mvc.perform(put("/tasks/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message", containsString("Cannot cancel task in state")));
    }
}
