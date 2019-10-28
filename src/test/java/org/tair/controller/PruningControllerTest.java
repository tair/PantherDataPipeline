package org.tair.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

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
        this.mockMvc.perform(post("/panther/grafting")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content("{\"sequence\": \"MSKVRDRTEDFRDAVRVAALSHGYTESQLAALMASFIMHKAPWRSAFMKAALKTLESIKE" +
                                            "LERFIVKHRKDYVDLHRTTEQERDNIEHEVAVFVKVCKDQIDILKNRIHDEETEGSGRTW" +
                                            "LQFRDDASHADMVAHKHGVVLILSEKLHSVTAQFDQLRSIRFQDAMNRVMPRRKVHRLPQ" +
                                            "PKSEASKSDLPKLGEQELSSGTIRVQEQLLDDETRALQVELTNLLDAVQETETKMVEMSA" +
                                            "LNHLMSTHVLQQAQQIEHLYEQAVEATNNVVLGNKELSQAIKRNSSSRTFLLLFFVVLTF" +
                                            "SILFLDWYS\"}"))
                .andExpect(status().isOk());
    }

    @Test
    public void shouldReturnPrunedGraftedTree() throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sequence", "MSKVRDRTEDFRDAVRVAALSHGYTESQLAALMASFIMHKAPWRSAFMKAALKTLESIKE" +
                "LERFIVKHRKDYVDLHRTTEQERDNIEHEVAVFVKVCKDQIDILKNRIHDEETEGSGRTW" +
                "LQFRDDASHADMVAHKHGVVLILSEKLHSVTAQFDQLRSIRFQDAMNRVMPRRKVHRLPQ" +
                "PKSEASKSDLPKLGEQELSSGTIRVQEQLLDDETRALQVELTNLLDAVQETETKMVEMSA" +
                "LNHLMSTHVLQQAQQIEHLYEQAVEATNNVVLGNKELSQAIKRNSSSRTFLLLFFVVLTF" +
                "SILFLDWYS");
        jsonObject.put("taxonIdsToShow", Arrays.asList("2711"));
        String payload = jsonObject.toString();
        ObjectNode requestJson = (ObjectNode) new ObjectMapper().readTree(payload);

        this.mockMvc.perform(post("/panther/grafting/prune")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isOk());
    }
}
