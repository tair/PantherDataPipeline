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
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.tair.module.GOAnnotationData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class UpdateGOAnnotations {
	String solrUrl = "http://localhost:8983/solr";
	SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
	ObjectMapper mapper = new ObjectMapper();
	
	public void updateGOAnnotations() throws SolrServerException, IOException, InterruptedException {
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("id","uniprot_ids");
		query.setSort("id", ORDER.asc);
		
		QueryResponse tempResponse = solrClient.query("panther", query);
		int total = (int)tempResponse.getResults().getNumFound();
		query.setRows(total);
		QueryResponse response = solrClient.query("panther", query);

		for (SolrDocument result : response.getResults()) {
			Collection<Object> uniprotIds = result.getFieldValues("uniprot_ids");
			if (uniprotIds == null) continue;
			String id = (String) result.getFieldValue("id");
			System.out.println("Processing: "+id);
			List<String> goAnnotationDataList = getGOAnnotationsForTree(uniprotIds);
			
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
	
	public List<String> getGOAnnotationsForTree(Collection<Object> uniprotIds) throws SolrServerException, IOException, InterruptedException {
		SolrQuery query = new SolrQuery("*:*");
		List<String> goAnnotationDataList = new ArrayList<String>();
		for (Object uniprotId : uniprotIds) {
			query.setQuery("uniprot_id:"+uniprotId.toString().toLowerCase());		
			QueryResponse response = solrClient.query("uniprot_db", query);
			SolrDocumentList results = response.getResults();
			List<String> goAnnotations = new ArrayList<String>();
			for (SolrDocument result: results) {
				goAnnotations.add((String) result.getFieldValue("go_annotations"));
			}
			if (goAnnotations.size()>0) {
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
		
		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);
	}
}
