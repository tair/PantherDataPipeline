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
public class Reference<T> {
    private List<Publication> citation = new ArrayList<>();

    @JsonProperty("citation")
    private void buildSequenceInfo(T citation) throws JsonProcessingException {
        if (citation instanceof List) {
            this.citation = (List<Publication>) citation;
        } else {
            final ObjectMapper mapper = new ObjectMapper(); // jackson's objectmapper
            final Publication citationObj = mapper.convertValue(citation, Publication.class);
            this.citation.add(citationObj);
        }
    }
}