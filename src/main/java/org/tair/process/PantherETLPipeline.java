package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.json.JSONArray;
import org.json.JSONObject;

import org.slf4j.LoggerFactory;
import org.tair.module.FamilyName;
import org.tair.module.PantherData;
import org.tair.module.PantherFamilyList;
import org.tair.module.PantherFamilyNameList;
import org.tair.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


import java.util.stream.Stream;


public class PantherETLPipeline {

	String urlString = "http://localhost:8983/solr/panther";
//	String urlString = "http://54.68.67.235:8983/solr/panther";
	String localPantherBooksFolder = "/Users/swapp1990/Documents/projects/Pheonix_Projects/pruned_panther_files/";
	SolrClient solr = new HttpSolrClient.Builder(urlString).build();
	ObjectMapper mapper = new ObjectMapper();
	int committedCount = 0;

	//Get Panther Family List from panther web server and convert to a json string and save locally
	public String getPantherFamilyListFromServer() throws Exception {
		String url = "http://panthertest1.med.usc.edu:8081/tempFamilySearch?type=family_list";
		return Util.readContentFromWebUrlToJsonString(url);
	}

	public void savePantherFamilyListJsonLocally(String jsonString) throws Exception {
		String filePath = "src/main/resources/panther/familyList.json";
		File jsonFile = new File(filePath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, jsonString);
	}

	//Get Panther Family List from local file if it exists
	public List<String> getLocalPantherFamilyList() throws Exception {
		String filePath = "src/main/resources/panther/familyList.json";
		InputStream input = new FileInputStream(filePath);

		String data = mapper.readValue(input, String.class);

		PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyList.class);
		return flJson.getFamilyList();
	}

	//Get Panther Family Name List from local file if it exists
	public Map<String, String> getLocalPantherFamilyNamesList() throws Exception {
		String filePath = "src/main/resources/panther/familyNamesList.json";
		InputStream input = new FileInputStream(filePath);

		String data = mapper.readValue(input, String.class);

		PantherFamilyNameList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyNameList.class);

		Map<String, String> idToFamilyName = new HashMap<String, String>();
		for(int i = 0; i < flJson.getFamilyNames().size(); i++) {
			FamilyName familyNameObj = flJson.getFamilyNames().get(i);
			idToFamilyName.put(familyNameObj.getPantherId(), familyNameObj.getFamilyName());
		}
		return idToFamilyName;
	}

	public void savePantherTreeLocallyById(String familyId, int idx) throws Exception {
		PantherData pantherData = new PantherBookXmlToJson().readPantherTreeById(familyId);
		String filePath = localPantherBooksFolder + familyId+ ".json";
		try {
			savePantherJsonToLocal(filePath, pantherData);
			System.out.println("Saved: "+pantherData.getId() + " idx: " + idx);
		} catch(Exception e) {
			System.out.println("Error in saving "+pantherData.getId());
		}
	}

	public void savePantherTreesLocally(List<String> familyList) throws Exception {
		for(int i = 3500; i < familyList.size(); i++) {
			String familyId = familyList.get(i);
			savePantherTreeLocallyById(familyId, i);
		}
	}

	public void readPantherBooksList() throws Exception {

		StringBuilder data = new StringBuilder();
		String fileName = "/Users/swapp1990/Documents/projects/Pheonix_Projects/PantherPipeline/src/main/java/org/tair/module/panther_books.html";
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
//			System.out.println("Stored " + id);
		}
		saveAndCommitToSolr(pantherList);
		pantherList.clear();
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Success!");
	}

	public void indexSolrDB(Map<String, String> idToFamilyNames) throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		List<PantherData> pantherList = new ArrayList<>();
		List<String> emptyPantherIds = new ArrayList<>();
		int commitCount = 500;
		for(int i = 0; i < 5; i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			String familyName = idToFamilyNames.get(pantherFamilyList.get(i));
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);
			if(modiPantherData != null) {
				pantherList.add(modiPantherData);
			} else {
				emptyPantherIds.add(origPantherData.getId());
			}
			if(pantherList.size() >= commitCount) {
				saveAndCommitToSolr(pantherList);
				pantherList.clear();
			}
		}
		saveAndCommitToSolr(pantherList);
		pantherList.clear();
	}

	public void savePantherFamilyNamesLocally() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		JSONObject jo = new JSONObject();
		Collection<JSONObject> items = new ArrayList<JSONObject>();
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			JSONObject item = new JSONObject();
			item.put("pantherId", pantherFamilyList.get(i));
			item.put("familyName", readFamilyNameFromUrl(pantherFamilyList.get(i)));
			items.add(item);
			if(items.size() % 100 == 0) {
				System.out.println(items.size() + " items added");
			}
		}
		String filePath = "src/main/resources/panther/familyNamesList.json";
		File jsonFile = new File(filePath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		jo.put("familyNames", new JSONArray(items));

		mapper.writeValue(jsonFile, jo.toString());
	}

	public String readFamilyNameFromUrl(String family_id) throws Exception {
		String flUrl = "http://panthertest1.med.usc.edu:8081/tempFamilySearch?type=family_name&book=" + family_id;
		String jsonString = Util.readFamilyNameFromUrl(flUrl);
		String familyName = new JSONObject(jsonString).getJSONObject("search").getString("family_name");
		return familyName;
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
		committedCount += pantherList.size();
		System.out.println("Commit! "+committedCount);
	}

	public static void main(String args[]) throws Exception {
		PantherETLPipeline etl = new PantherETLPipeline();

//		etl.readPantherBooksList();
//		String familyListJson = etl.getPantherFamilyListFromServer();
//		etl.savePantherFamilyListJsonLocally(familyListJson);
// 		etl.savePantherFamilyNamesLocally();
//		List<String> pantherFamilyList = etl.getLocalPantherFamilyList();
//		etl.savePantherTreesLocally(pantherFamilyList);

		Map<String, String> idToFamilyNames = etl.getLocalPantherFamilyNamesList();
		etl.indexSolrDB(idToFamilyNames);
	}

}
