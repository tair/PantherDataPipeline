package org.tair.controller;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class PruningControllerTest {

    @Autowired
    private PruningController controller;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void shouldReturnDefault() throws Exception {
        this.mockMvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(content().string("myPage"));
    }

    @Test
    public void shouldReturnGraftedTree() throws Exception {
        SequenceObj sequenceObj = new SequenceObj();
        sequenceObj.setSequence("test");
        this.mockMvc.perform(post("/panther/grafting")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content("{\"sequence\": \"test\"}"))
                .andExpect(status().isOk());
    }
}
