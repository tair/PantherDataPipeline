/*** 
 * Load go annotations from api into solr's uniprot_db collection
 ***/

package org.tair.process.uniprotdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.tair.module.GOAnnotationData;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GOAnnotationETLPipeline {

	String solrUrl = "http://localhost:8983/solr";
	SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
	ObjectMapper mapper = new ObjectMapper();
	GOAnnotationUrlToJson GOAnnotationUrlToJson = new GOAnnotationUrlToJson();

	public void storeGOAnnotationToUniprotDb() throws Exception {
		
		final SolrQuery query = new SolrQuery("*:*");
		query.setRows(0);
		query.setFacet(true);
		query.addFacetField("uniprot_ids");
		query.setFacetLimit(-1); // -1 means unlimited
		
		final QueryResponse response = solrClient.query("panther", query);
		final FacetField uniprot_ids = response.getFacetField("uniprot_ids");
		List<Count> counts = uniprot_ids.getValues();
		System.out.println("Total number to load: " + counts.size());

		for (int i = 0; i<counts.size()/500+1; i++) {
			List<String> uniprotIdList = new ArrayList<String>();
			for (int j =i*500; j<(i+1)*500; j++) {
				if(j== counts.size()) 
					break;
				uniprotIdList.add(counts.get(j).getName());
			}

			System.out.println("Loading: " + i*500 +"-" +(i+1)*500);
			
			List<GOAnnotationData> goAnnotations = GOAnnotationUrlToJson.readGOAnnotationUrlToObjectList(String.join(",", uniprotIdList));
			if (goAnnotations.size() >0) {
				solrClient.addBeans("uniprot_db",goAnnotations);
		        solrClient.commit("uniprot_db");
			}
		}
		System.out.println("finished commits to uniprot_db collection");

	}

	public static void main(String args[]) throws Exception {
		long startTime = System.nanoTime();

		new GOAnnotationETLPipeline().storeGOAnnotationToUniprotDb();
		
		long endTime = System.nanoTime();		
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in nanoseconds  : " + timeElapsed);
		System.out.println("Execution time in milliseconds : " + 
								timeElapsed / 1000000);
	}
}
