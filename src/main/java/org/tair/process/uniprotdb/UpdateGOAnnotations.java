package org.tair.process.uniprotdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.FacetParams;
import org.tair.module.GOAnnotationData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class UpdateGOAnnotations {
	String solrUrl = "http://localhost:8983/solr";
	SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
	ObjectMapper mapper = new ObjectMapper();
	int uniprot_rows;
	
	public void updateGOAnnotations() throws SolrServerException, IOException, InterruptedException {
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("id","uniprot_ids");
		query.setSort("id", ORDER.asc);

		QueryResponse tempResponse = solrClient.query("panther", query);
		int total = (int)tempResponse.getResults().getNumFound();
		query.setRows(total);
		QueryResponse response = solrClient.query("panther", query);

		//using facet to get uniprot_db's max length result, and set the number to uniprot_db's rows.
		SolrQuery uniprotFacetQuery = new SolrQuery("*:*");
		uniprotFacetQuery.setRows(0);
		uniprotFacetQuery.setFacet(true);
		uniprotFacetQuery.addFacetField("uniprot_id");
		uniprotFacetQuery.setFacetLimit(-1); // -1 means unlimited
		uniprotFacetQuery.setFacetSort(FacetParams.FACET_SORT_COUNT);

		QueryResponse uniprotFacetResponse = solrClient.query("paint_db", uniprotFacetQuery);
		FacetField uniprotIdFacets = uniprotFacetResponse.getFacetField("uniprot_id");
		uniprot_rows = (int)uniprotIdFacets.getValues().get(0).getCount();
		System.out.println("Uniprot DB result rows set to: " + uniprot_rows);

		for (int i = 0; i<response.getResults().size(); i ++) {
			SolrDocument result = response.getResults().get(i);
			Collection<Object> uniprotIds = result.getFieldValues("uniprot_ids");
			String id = (String) result.getFieldValue("id");
			System.out.println("Processing: "+id +" idx: "+i);
			List<String> goAnnotationDataList = getGOAnnotationsForTree(uniprotIds);
			System.out.println(goAnnotationDataList.size());
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", id);
			Map<String, List<String>> partialUpdate = new HashMap<>();
			partialUpdate.put("set", goAnnotationDataList);
			doc.addField("go_annotations", partialUpdate);
			solrClient.add("panther", doc);
			solrClient.commit("panther");
			System.out.println("commited: "+ id);
		}
	}

	public void testUniprot() throws SolrServerException, IOException {
		String uniprotId = "Q7JR71";
		SolrQuery query = new SolrQuery("*:*");
		query.setQuery("uniprot_id:"+uniprotId.toString().toLowerCase());
		query.setRows(uniprot_rows);
		QueryResponse response = solrClient.query("uniprot_db", query);
		SolrDocumentList results = response.getResults();
		System.out.println(uniprotId.toString().toLowerCase() + results.getNumFound());
	}
	
	public List<String> getGOAnnotationsForTree(Collection<Object> uniprotIds) throws SolrServerException, IOException, InterruptedException {
		SolrQuery query = new SolrQuery("*:*");
		List<String> goAnnotationDataList = new ArrayList<String>();
//		System.out.println("uniprotIds "+ uniprotIds.size());
		for (Object uniprotId : uniprotIds) {
			query.setQuery("uniprot_id:"+uniprotId.toString().toUpperCase());
			query.setRows(uniprot_rows);
			QueryResponse response1 = solrClient.query("paint_db", query);
			query.setQuery("uniprot_id:"+uniprotId.toString().toLowerCase());
			query.setRows(uniprot_rows);
			QueryResponse response2 = solrClient.query("uniprot_db", query);
			List<String> goAnnotations = new ArrayList<String>();

			for (SolrDocument result: response1.getResults()) {
				goAnnotations.add((String) result.getFieldValue("go_annotations"));
			}
			for (SolrDocument result: response2.getResults()) {
				goAnnotations.add((String) result.getFieldValue("go_annotations"));
			}
			if (goAnnotations.size()>0) {
//				System.out.println(uniprotId.toString().toLowerCase() + " results1: " + response1.getResults().getNumFound() + ", " + response2.getResults().getNumFound());
//				System.out.println(goAnnotations.toString());
				GOAnnotationData goAnnotationData = new GOAnnotationData();
				goAnnotationData.setGo_annotations(goAnnotations.toString());
				goAnnotationData.setUniprot_id(uniprotId.toString().toLowerCase());
				ObjectWriter ow = new ObjectMapper().writer();
				String goAnnotationDataStr = ow.writeValueAsString(goAnnotationData);
				goAnnotationDataList.add(goAnnotationDataStr);
			}
		}
		return goAnnotationDataList;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.nanoTime();

		UpdateGOAnnotations UpdateGOAnnotations= new UpdateGOAnnotations();
		UpdateGOAnnotations.updateGOAnnotations();
//		UpdateGOAnnotations.testUniprot();

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);
	}
}
