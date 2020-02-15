package org.tair.module.pantherForPhylo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Search {
    private Parameters parameters;
    private Annotation annotation_node;

    public Annotation getAnnotation_node() {
        return annotation_node;
    }

    public Parameters getParameters() {
        return parameters;
    }
}
