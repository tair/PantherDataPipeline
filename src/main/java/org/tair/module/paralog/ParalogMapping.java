package org.tair.module.paralog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ParalogMapping {
    private ParaSearchResult search;

    public List<Mapped> getAllMappedParalogs() {
        return this.search.mapping.getMappedList();
    }
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class ParaSearchResult {
        /**
         * "product": {
            "version": 16, "content": "PANTHERDB"
            },
            "mapping": {
                "mapped": []
            }
         **/

    private Product product;
    public Mapping mapping;
}

@Data
class Product {
    private int version;
    private String content;
}

@Data
class Mapping {
    @JsonProperty("mapped")
    private JsonNode mapped;
    private List<Mapped> mappedList;
    private Mapped singleMapped;

    @JsonSetter("mapped")
    public void setMapped(JsonNode mapped) {
        if (mapped instanceof ArrayNode) {
            ObjectMapper mapper = new ObjectMapper();
            // acquire reader for the right type
            ObjectReader reader = mapper.readerFor(new TypeReference<List<Mapped>>() {});
            try {
                this.mappedList = reader.readValue(mapped);
            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            this.mappedList = new ArrayList<>();
//            ObjectMapper mapper = new ObjectMapper();
//            // acquire reader for the right type
//            ObjectReader reader = mapper.readerFor(new TypeReference<Mapped>() {});
//            try {
//                Mapped single_mapped = reader.readValue(mapped);
//                this.mappedList = new ArrayList<>();
//                this.mappedList.add(single_mapped);
//            } catch (Exception e) {
//                System.out.println(e);
//            }
        }
    }

    public List<Mapped> getMappedList() {
        return mappedList;
    }

    public Mapped getSingleMapped() {
        return singleMapped;
    }
}

