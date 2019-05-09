package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PantherData {

	private SearchResult search;

	@Field
	private String id;
	@Field
	private String family_name;
	@Field
	private String jsonString;
	@Field
	private List<String> event_types = new ArrayList<String>();
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
	private List<String> speciation_events = new ArrayList<String>();
	@Field
	private List<String> species_list = new ArrayList<String>();
}