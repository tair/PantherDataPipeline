package org.tair.module;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SequenceList<T> {
    private List<String> sequence_info = new ArrayList<>();
    private ObjectWriter ow = new ObjectMapper().writer();

    @JsonProperty("sequence_info")
    private void buildSequenceInfo(T sequence_info) throws JsonProcessingException {
        if (sequence_info instanceof List) {
            List<Object> sequenceInfoList = (List<Object>) sequence_info;
            for (Object sequenceInfoObj : sequenceInfoList) {
                this.sequence_info.add(ow.writeValueAsString(sequenceInfoObj));
            }
        } else {
            this.sequence_info.add(ow.writeValueAsString(sequence_info));
        }
    }
}
