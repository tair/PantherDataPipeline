package org.tair.module.panther;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.tair.module.SequenceList;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchResult {
	private Tree tree_topology;

	private SequenceList sequence_list;

	public Annotation getAnnotation_node() {
		if(tree_topology == null || tree_topology.getAnnotation_node() == null) return null;
		return tree_topology.getAnnotation_node();
	}
}

@Data
@JsonInclude(Include.NON_NULL)
//@JsonIgnoreProperties(ignoreUnknown = true)
class Tree {
	private Annotation annotation_node;
}