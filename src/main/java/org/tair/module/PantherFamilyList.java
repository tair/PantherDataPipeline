package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.apache.solr.client.solrj.beans.Field;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PantherFamilyList {
    Search search;

    public List<FamilyNode> getFamilyNodes() {
        return search.getPanther_family_subfam_list().getFamily();
    }
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class Search {
    FamilyList panther_family_subfam_list;
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class FamilyList {
    List<FamilyNode> family;
}

