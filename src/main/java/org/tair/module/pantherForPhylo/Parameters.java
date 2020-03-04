package org.tair.module.pantherForPhylo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Parameters {
    private String elapsed_time;
    private String book;

    public String getBook() {
        return book;
    }

    public String getElapsed_time() {
        return elapsed_time;
    }
}
