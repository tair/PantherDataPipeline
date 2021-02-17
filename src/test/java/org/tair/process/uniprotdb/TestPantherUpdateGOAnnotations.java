package org.tair.process.uniprotdb;

//import java.awt.Toolkit;
//import java.awt.datatransfer.Clipboard;
//import java.awt.datatransfer.StringSelection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tair.process.panther.PantherUpdateGOAnnotations;

public class TestPantherUpdateGOAnnotations {
	String solrUrl = "http://localhost:8983/solr";
	SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
	ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void testSolrQuery() throws SolrServerException, IOException {
		final SolrQuery query = new SolrQuery("*:*");
		query.setFields("id","uniprot_ids");
		PantherUpdateGOAnnotations PantherUpdateGOAnnotations = new PantherUpdateGOAnnotations();
		PantherUpdateGOAnnotations.solrClient.query("panther", query);
	}
	
	@Test
	public void TestGetGOAnnotationsForTree() throws SolrServerException, IOException, InterruptedException {
		Collection<Object> uniprotIds = new ArrayList<Object>();
		uniprotIds.add("A0A0B4JCZ8");
		uniprotIds.add("Q23624");
		System.out.println(new PantherUpdateGOAnnotations().getGOAnnotationsForTree(uniprotIds));
	}
	
	@Test
	public void TestAtomicUpdate() throws SolrServerException, IOException {		
	SolrInputDocument doc = new SolrInputDocument();
	doc.addField("id", "PTHR10004");
	Map<String, List<String>> partialUpdate = new HashMap<>();
	List<String> stringList = new ArrayList<String>();
	stringList.add("test");
	stringList.add("atomic updates");
	partialUpdate.put("set", stringList);
	doc.addField("go_annotations", partialUpdate);
	solrClient.add("panther", doc);
	solrClient.commit("panther");
	}
	
//	@Test
//	public void test() throws SolrServerException, IOException {
//		final SolrQuery query = new SolrQuery("id:PTHR10015");
//		query.setFields("uniprot_ids");
//		PantherUpdateGOAnnotations PantherUpdateGOAnnotations= new PantherUpdateGOAnnotations();
//		final QueryResponse response = PantherUpdateGOAnnotations.solrClient.query("panther", query);
//		List<String> uniprotIds = new ArrayList<String>();
//		for (Object uniprotId : response.getResults().get(0).getFieldValues("uniprot_ids")) {
//			uniprotIds.add(uniprotId.toString().toLowerCase());
//		}
//		System.out.println(String.join(" ", uniprotIds));
//		StringSelection selection = new StringSelection(String.join(" ", uniprotIds));
//		Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
//		clipboard.setContents(selection, selection);
//	}

	
}
