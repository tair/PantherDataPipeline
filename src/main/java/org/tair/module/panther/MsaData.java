package org.tair.module.panther;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MsaData {
    private MSASearchResult search;
    public List<MSASequenceInfo> getSequenceList() {
//        if(search.getMSA_list().)
        return search.getMSA_list().getSequence_info();
    }
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class MSASearchResult {
    @JsonProperty("MSA_list")
    private MSAList MSA_list;
}
