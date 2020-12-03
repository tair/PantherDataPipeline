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

	public String get_uniprotId() {
		if(node_name == null) return null;
		return node_name.split("UniProtKB=")[1];
	}
	public String get_geneCode() {
		if(gene_id == null) return null;
		String extracted_gene_id = gene_id.split("\\:")[1];
		String code = extracted_gene_id.split("=", 2)[0];
		return code;
	}
	public String get_extractedGeneId() {
		if(gene_id == null) return null;
		String extracted_gene_id = gene_id.split(":")[1];
		return extracted_gene_id;
	}
}
