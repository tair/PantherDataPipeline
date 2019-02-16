package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.json.JSONArray;
import org.json.JSONObject;

import org.tair.module.FamilyName;
import org.tair.module.PantherData;
import org.tair.module.PantherFamilyList;
import org.tair.module.PantherFamilyNameList;
import org.tair.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class PantherETLPipeline {

	private String URL_SOLR = "http://localhost:8983/solr/panther";
//	String URL_SOLR = "http://54.68.67.235:8983/solr/panther";
	private String URL_PTHR_FAMILY_LIST = "http://panthertest1.med.usc.edu:8081/tempFamilySearch?type=family_list";
	private String URL_PTHR_FAMILY_NAME = "http://panthertest1.med.usc.edu:8081/tempFamilySearch?type=family_name&book=";

	//Change this to the location of pruned panther files that you have saved locally
	private String PATH_LOCAL_PRUNED_TREES = "src/main/resources/panther/pruned_panther_files/";
	private String PATH_FAMILY_LIST = "src/main/resources/panther/familyList.json";
	private String PATH_FAMILY_NAMES_LIST = "src/main/resources/panther/familyNamesList.json";

	SolrClient solr = new HttpSolrClient.Builder(URL_SOLR).build();
	ObjectMapper mapper = new ObjectMapper();
	int committedCount = 0;

	public void updateOrSaveFamilyListJson() throws Exception{
		String familyListJson = getPantherFamilyListFromServer();
		savePantherFamilyListJsonLocally(familyListJson);
	}

	//Get Panther Family List from panther web server and convert to a json string and save locally
	public String getPantherFamilyListFromServer() throws Exception {
		return Util.readContentFromWebUrlToJsonString(URL_PTHR_FAMILY_LIST);
	}

	//Get Panther Family Name for given id using panther url
	public String getFamilyNameFromServer(String family_id) throws Exception {
		String flUrl = URL_PTHR_FAMILY_NAME + family_id;
		String jsonString = Util.readFamilyNameFromUrl(flUrl);
		String familyName = new JSONObject(jsonString).getJSONObject("search").getString("family_name");
		return familyName;
	}

	//Save list of Panther family ids.
	public void savePantherFamilyListJsonLocally(String jsonString) throws Exception {
		File jsonFile = new File(PATH_FAMILY_LIST);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, jsonString);
	}

	//Save panther family names for panther ids.
	public void savePantherFamilyNamesLocally() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		JSONObject jo = new JSONObject();
		Collection<JSONObject> items = new ArrayList<JSONObject>();
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			JSONObject item = new JSONObject();
			item.put("pantherId", pantherFamilyList.get(i));
			item.put("familyName", getFamilyNameFromServer(pantherFamilyList.get(i)));
			items.add(item);
			if(items.size() % 100 == 0) {
				System.out.println(items.size() + " items added");
			}
		}

		File jsonFile = new File(PATH_FAMILY_NAMES_LIST);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		jo.put("familyNames", new JSONArray(items));

		mapper.writeValue(jsonFile, jo.toString());
	}

	public void savePantherTreesLocally(List<String> familyList) throws Exception {
		for(int i = 0; i < familyList.size(); i++) {
			String familyId = familyList.get(i);
			savePantherTreeLocallyById(familyId, i);
		}
	}

	public void savePantherTreeLocallyById(String familyId, int idx) throws Exception {
		PantherData pantherData = new PantherBookXmlToJson().readPantherTreeById(familyId);
		String filePath = PATH_LOCAL_PRUNED_TREES + familyId+ ".json";
		try {
			savePantherJsonToLocal(filePath, pantherData);
			System.out.println("Saved: "+pantherData.getId() + " idx: " + idx);
		} catch(Exception e) {
			System.out.println("Error in saving "+pantherData.getId());
		}
	}

	//Get Panther Family List from local file if it exists
	public List<String> getLocalPantherFamilyList() throws Exception {
		InputStream input = new FileInputStream(PATH_FAMILY_LIST);

		String data = mapper.readValue(input, String.class);

		PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyList.class);
		return flJson.getFamilyList();
	}

	//Get Panther Family Name List from local file if it exists
	public Map<String, String> getLocalPantherFamilyNamesList() throws Exception {
		InputStream input = new FileInputStream(PATH_FAMILY_NAMES_LIST);

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

	//Reindex Solr DB with modified panther data
	public void indexSolrDB(Map<String, String> idToFamilyNames) throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		List<PantherData> pantherList = new ArrayList<>();
		List<String> emptyPantherIds = new ArrayList<>();
		int commitCount = 1;
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			String familyName = idToFamilyNames.get(pantherFamilyList.get(i));
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);

			//Some panther trees might be empty after pruning, so we should not add it to solr.
			// We save the empty panther tree ids inside "emptyPantherIds"
			if(modiPantherData != null) {
				pantherList.add(modiPantherData);
			} else {
				emptyPantherIds.add(origPantherData.getId());
			}

			if(pantherList.size() >= commitCount) {
				System.out.println(modiPantherData.getId() + " idx: " + i + " size: " + modiPantherData.getJsonString().length());
				saveAndCommitToSolr(pantherList);
				pantherList.clear();
			}
		}
		saveAndCommitToSolr(pantherList);
		pantherList.clear();
	}

	public PantherData readPantherBooksFromLocal(String id) throws Exception {
		String filePath = PATH_LOCAL_PRUNED_TREES + id + ".json";
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

		//Run the following if we don't have a local family list json, or it needs to be updated
//		etl.updateOrSaveFamilyListJson();

		//Run the following If we don't have a local family names list, or it needs to be updated
// 		etl.savePantherFamilyNamesLocally();

		//Run the following If we don't have downloaded local panther trees, or it needs to be updated
//		List<String> pantherFamilyList = etl.getLocalPantherFamilyList();
//		etl.savePantherTreesLocally(pantherFamilyList);

		//Reindex Solr DB based on local panther files and change in solr schema.
		Map<String, String> idToFamilyNames = etl.getLocalPantherFamilyNamesList();
		etl.indexSolrDB(idToFamilyNames);
	}

}
