package org.tair.module.panther;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchResult {
	private Tree tree_topology;

	private Annotation annotation_node;

	private MSAList MSA_list;

	public Annotation getAnnotation_node() {
		if(annotation_node != null) return annotation_node;
		if(tree_topology == null || tree_topology.getAnnotation_node() == null) return null;
		return tree_topology.getAnnotation_node();
	}

	public void setAnnotation_node(Annotation root) {
		if(tree_topology == null) return;
		tree_topology.setAnnotation_node(root);
	}
}

@Data
@JsonInclude(Include.NON_NULL)
//@JsonIgnoreProperties(ignoreUnknown = true)
class Tree {
	private Annotation annotation_node;
}