package org.tair.module.panther;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.tair.module.Children;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Annotation {
	//Total unique variables: 15
	//Common Node vars
	private String prop_sf_id;
	private String tree_node_type;
	private String branch_length;
	private String sf_name;
	private String persistent_id;
	//Internal Node variables
//	private String accession;
	private String sf_id;
	private String definition;
	private String species;
	private String taxonomic_range;
	private String event_type;
	private Children children;
	//Leaf Node variables
	private String organism;
	private String gene_id;
	private String gene_symbol;
	private String node_name;

	//Ignored variables
//	private String PANTHER_GO_SLIM_CC;

}
