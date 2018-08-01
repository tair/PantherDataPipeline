package org.tair.module;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
public class PantherData {

	private SearchResult search;

	@Field
	private String id;
	@Field
	private String family_name;
	@Field
	private String jsonString;
	@Field
	private List<String> node_types = new ArrayList<String>();
	@Field
	private List<String> organisms = new ArrayList<String>();
	@Field
	private List<String> gene_symbols = new ArrayList<String>();
	@Field
	private List<String> node_names = new ArrayList<String>();
	@Field
	private List<String> uniprot_ids = new ArrayList<String>();
	@Field
	private List<String> definitions = new ArrayList<String>();
	@Field
	private List<String> branch_lengths = new ArrayList<String>();
	@Field
	private List<String> accessions = new ArrayList<String>();
	@Field
	private List<String> gene_ids = new ArrayList<String>();
	@Field
	private List<String> sf_names = new ArrayList<String>();
	@Field
	private List<String> sf_ids = new ArrayList<String>();
	@Field
	private List<String> reference_speciation_events = new ArrayList<String>();
	@Field
	private List<String> species_list = new ArrayList<String>();
	
	// MSA information
	@Field
	private String msaJsonString;

}