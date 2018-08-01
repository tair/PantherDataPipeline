package org.tair.module;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
public class SearchResult {
	private Parameters parameters;
	private Annotation annotation_node;
	
	private SequenceList sequence_list;
}