package org.tair.process.panther;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.FacetParams;
import org.tair.module.GOAnnotationData;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class PantherUpdateGOAnnotations {
	private String RESOURCES_DIR = "src/main/resources";
	private String BASE_SOLR_URL = "http://localhost:8983/solr";
	public SolrClient solrClient = null;
	int uniprot_rows;

	public PantherUpdateGOAnnotations() {
		loadProps();
		solrClient = new HttpSolrClient.Builder(BASE_SOLR_URL).build();
	}

	private void loadProps() {
		try {
			InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
			// load props
			Properties prop = new Properties();
			prop.load(input);
			if (prop.containsKey("BASE_SOLR_URL")) {
				BASE_SOLR_URL = prop.getProperty("BASE_SOLR_URL");
				System.out.println(BASE_SOLR_URL);
			}
		}
		catch (Exception e) {
			System.out.println("Prop file not found!");
		}
	}

	public void getGoAnnotations() throws Exception {
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("id","go_annotations");
		query.setSort("id", ORDER.asc);
		QueryResponse tempResponse = solrClient.query("panther", query);
		int total = (int)tempResponse.getResults().getNumFound();
		query.setRows(total);
		QueryResponse response = solrClient.query("panther", query);
		int count = 0;
		for (int i = 0; i<response.getResults().size(); i ++) {
			SolrDocument result = response.getResults().get(i);
			String id = (String) result.getFieldValue("id");
			Collection<Object> go_annos = result.getFieldValues("go_annotations");
			if(go_annos != null) {
				System.out.println("Found anno for id "+ id);
				count++;
			}
		}
		System.out.println("Total trees found " + count);
	}

	public void updateGOAnnotations_selected(String[] sel_ids) throws SolrServerException, IOException, InterruptedException {
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
			for(int j=0; j<sel_ids.length;j++) {
				if(id.equals(sel_ids[j])) {
					System.out.println("Processing: " + id + " idx: " + i);
					List<String> goAnnotationDataList = getGOAnnotationsForTree(uniprotIds);
					System.out.println(goAnnotationDataList.size());
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", id);
					Map<String, List<String>> partialUpdate = new HashMap<>();
					partialUpdate.put("set", goAnnotationDataList);
					doc.addField("go_annotations", partialUpdate);
					solrClient.add("panther", doc);
					solrClient.commit("panther");
					System.out.println("commited: " + id);
				}
			}
		}
	}
	
	public void updateGOAnnotations() throws SolrServerException, IOException, InterruptedException {
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("id","uniprot_ids","go_annotations");
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
			System.out.println("Processing: " + id + " idx: " + i);
			Collection<Object> go_annos = result.getFieldValues("go_annotations");
//			if(go_annos == null) {
				List<String> goAnnotationDataList = getGOAnnotationsForTree(uniprotIds);
				System.out.println(goAnnotationDataList.size());
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("id", id);
				Map<String, List<String>> partialUpdate = new HashMap<>();
				partialUpdate.put("set", goAnnotationDataList);
				doc.addField("go_annotations", partialUpdate);
				solrClient.add("panther", doc);
				solrClient.commit("panther");
				System.out.println("commited: " + id);
//			} else {
//				System.out.println("Go annotations already added");
//			}
		}
	}
	
	public List<String> getGOAnnotationsForTree(Collection<Object> uniprotIds) throws SolrServerException, IOException {
//		SolrQuery query = new SolrQuery("*:*");
		SolrQuery query = new SolrQuery("*:*");
		query.setFields("go_annotations");
		List<String> goAnnotationDataList = new ArrayList<String>();
		System.out.println("uniprotIds "+ uniprotIds.size());
		for (Object uniprotId : uniprotIds) {
			System.out.println("uniprotId: "+ uniprotId.toString().toUpperCase());
			query.setQuery("uniprot_id:"+uniprotId.toString().toUpperCase());
			query.setRows(uniprot_rows);
			QueryResponse response1 = solrClient.query("paint_db", query);
			query.setQuery("uniprot_id:"+uniprotId.toString().toLowerCase());
			query.setRows(uniprot_rows);
			QueryResponse response2 = solrClient.query("uniprot_db", query);
			List<String> goAnnotations = new ArrayList<String>();
			System.out.println(response1.getResults());
			for (SolrDocument result: response1.getResults()) {
				System.out.println(result.getFieldValue("go_annotations"));
				goAnnotations.add((String) result.getFieldValue("go_annotations"));
			}
			for (SolrDocument result: response2.getResults()) {
				goAnnotations.add((String) result.getFieldValue("go_annotations"));
			}
			if (goAnnotations.size()>0) {
				System.out.println(uniprotId.toString().toLowerCase() + " results1: " + response1.getResults().getNumFound() + ", " + response2.getResults().getNumFound());
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

	public void testUniprot() throws SolrServerException, IOException {
		String uniprotId = "Q9Y5Z9";
		uniprot_rows = 56;
		Collection<Object> ids = new ArrayList<>();
		ids.add(uniprotId);
		List<String> annos = getGOAnnotationsForTree(ids);
		System.out.println(annos);
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.nanoTime();

		PantherUpdateGOAnnotations pt = new PantherUpdateGOAnnotations();
//		pantherUpdateGOAnnotations.getGoAnnotations();
		pt.updateGOAnnotations();
		// pt.testUniprot();

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);
	}
}
