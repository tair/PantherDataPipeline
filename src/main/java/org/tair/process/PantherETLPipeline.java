package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.tair.module.Annotation;
import org.tair.module.PantherData;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class PantherETLPipeline {

	String urlString = "http://localhost:8983/solr/panther";
//	String urlString = "http://54.68.67.235:8983/solr/panther";
	String localPantherBooksFolder = "/Users/swapp1990/Documents/projects/Pheonix_Projects/PantherPipeline/books2/";
	SolrClient solr = new HttpSolrClient.Builder(urlString).build();
	ObjectMapper mapper = new ObjectMapper();

	public void readPantherBooksList() throws Exception {

		StringBuilder data = new StringBuilder();
		String fileName = "/Users/swapp1990/Documents/projects/Pheonix_Projects/PantherDataPipeline/src/main/java/org/tair/module/panther_books.html";
		Stream<String> lines = Files.lines(Paths.get(fileName));
		lines.forEach(line -> data.append(line).append("\n"));
		lines.close();

		String arr[] = data.toString().split("<td><a href=\"");
		String last_id = "PTHR16632";

		boolean run = false;

		int commitCount = 500;
		List<PantherData> pantherList = new ArrayList<>();
		int count = arr.length;
		for (int i = 2; i < count; i++) {

			String id = arr[i].substring(0, arr[i].indexOf("/"));

//			PantherData pantherData = new PantherBookXmlToJson().readBookById(id);
//			System.out.println(pantherData);
//			String filePath = localPantherBooksFolder + id + ".json";
//			savePantherJsonToLocal(filePath, pantherData);
//			PantherData msaData = new PantherMsaXmlToJson().readMsaById(id);
//			pantherData.setMsaJsonString(msaData.getMsaJsonString());

			if(pantherList.size() >= commitCount) {
				saveAndCommitToSolr(pantherList);
				pantherList.clear();
			}
			PantherData pantherData = new PantherBookXmlToJson().readBookFromLocal(readPantherBooksFromLocal(id));

			pantherList.add(pantherData);
			System.out.println("Stored " + id);
		}
		saveAndCommitToSolr(pantherList);
		pantherList.clear();
		System.out.println("Success!");
	}

	public PantherData readPantherBooksFromLocal(String id) throws Exception {
		String filePath = localPantherBooksFolder + id + ".json";
		InputStream input = new FileInputStream(filePath);

		PantherData data = mapper.readValue(input, PantherData.class);
		return data;
	}

	public void savePantherJsonToLocal(String filePath, PantherData dataInJson) throws Exception {
		File jsonFile = new File(filePath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, dataInJson);
	}

	public void saveAndCommitToSolr(List<PantherData> pantherList) throws Exception{
		solr.addBeans(pantherList);
		solr.commit();
		System.out.println("Commit!");
	}

	public static void main(String args[]) throws Exception {

		PantherETLPipeline etl = new PantherETLPipeline();
		etl.readPantherBooksList();

	}

}
