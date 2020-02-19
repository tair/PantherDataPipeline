package org.tair.module.pantherForPhylo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Panther {
    private Search search;
    private String id;

    public String getId() {
        return id;
    }

    public Search getSearch() {
        return search;
    }
}