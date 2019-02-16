package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PantherFamilyList {
    private SearchResultFL search;

    public List<String> getFamilyList() {
        FamilyList fl = search.getFamily_list();
        return fl.getFamily_id();
    }
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class SearchResultFL {
    private Parameters parameters;
    private FamilyList family_list;
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class FamilyList {
    List<String> family_id = new ArrayList<>();
}
