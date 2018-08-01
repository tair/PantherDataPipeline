package org.tair.process;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.tair.module.PantherData;

public class PantherETLPipeline {

	String urlString = "http://localhost:8983/solr/panther";
	SolrClient solr = new HttpSolrClient.Builder(urlString).build();

	public void readPantherBooksList() throws Exception {

		StringBuilder data = new StringBuilder();
		String fileName = "/Users/trilok/code/pb/git/services/PantherDataPipeline/src/main/java/org/tair/module/panther_books.html";
		Stream<String> lines = Files.lines(Paths.get(fileName));
		lines.forEach(line -> data.append(line).append("\n"));
		lines.close();

		String arr[] = data.toString().split("<td><a href=\"");
//		String last_id = "PTHR16631";

		boolean run = false;
		for (int i = 2; i < arr.length; i++) {

			String id = arr[i].substring(0, arr[i].indexOf("/"));
//			if (id.equals(last_id))
//				run = true;
//
//			if (!run)
//				continue;

			PantherData pantherData = new PantherBookXmlToJson().readBookById(id);
//			PantherData msaData = new PantherMsaXmlToJson().readMsaById(id);
//			pantherData.setMsaJsonString(msaData.getMsaJsonString());
			
			solr.addBean(pantherData);
			solr.commit();
			System.out.println("Stored " + id);
		}

		System.out.println("Success!");
	}

	public static void main(String args[]) throws Exception {

		PantherETLPipeline etl = new PantherETLPipeline();
		etl.readPantherBooksList();

	}

}
