package org.tair.module;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
public class Annotation {

	private String node_type;
	private String organism;
	private String gene_symbol;
	private String node_name;
	private String definition;
	private String branch_length;
	private String accession;
	private String gene_id;
	private String sf_name;
	private String sf_id;
	private String reference_speciation_event;
	private String species;

	private Children children;
}
