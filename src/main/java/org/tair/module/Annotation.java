package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Annotation {

	//Internal Node variables
	private String accession;
	private String public_id;
	private String sf_id;
	private String sf_name;
	private String prop_sf_id;
	private String species;
	private String taxonomic_range;
	private String tree_node_type;
	private String event_type;
	private String branch_length;
	//Leaf Node variables
	private String gene_id;
	private String gene_symbol;
	private String definition;
	private String node_name;
	private String organism;

	private Children children;
}
