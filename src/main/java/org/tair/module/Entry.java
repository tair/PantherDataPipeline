package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Entry<T> {
    private List<Reference> reference = new ArrayList<>();

    @JsonProperty("reference")
    private void buildSequenceInfo(T reference) throws JsonProcessingException {
        if (reference instanceof List) {
            this.reference = (List<Reference>) reference;
        } else {
            final ObjectMapper mapper = new ObjectMapper(); // jackson's objectmapper
            final Reference refObj = mapper.convertValue(reference, Reference.class);
            this.reference.add(refObj);
        }
    }
}