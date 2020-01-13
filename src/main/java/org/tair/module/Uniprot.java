package org.tair.module;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Uniprot<T> {
    private List<Entry> entry = new ArrayList<>();

    @JsonProperty("entry")
    private void buildSequenceInfo(T entry) throws JsonProcessingException {
        if (entry instanceof List) {
            this.entry = (List<Entry>) entry;
        } else {
            final ObjectMapper mapper = new ObjectMapper(); // jackson's objectmapper
            final Entry entryObj = mapper.convertValue(entry, Entry.class);
            this.entry.add(entryObj);
        }
    }
}
