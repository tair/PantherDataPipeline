package org.tair.module.pantherForPhylo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Children {
    private List<Annotation> annotation_node; // anno_list

    public List<Annotation> getAnnotation_node() {
        return annotation_node;
    }
//    each children has two annotation_nodes and can be accessed by index 0 and 1
}
