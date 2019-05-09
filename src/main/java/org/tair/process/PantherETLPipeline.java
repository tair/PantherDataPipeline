package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.opencsv.CSVWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONObject;

import org.tair.module.*;
import org.tair.util.Util;

import java.io.*;
import java.util.*;

public class PantherETLPipeline {

	private String URL_SOLR = "http://localhost:8983/solr/panther";
//	String URL_SOLR = "http://54.68.67.235:8983/solr/panther";
	private String URL_PTHR_FAMILY_LIST = "http://pantherdb.org/tempFamilySearch?type=family_list&taxonFltr=13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,559292,284812";
	private String URL_PTHR_FAMILY_NAME = "http://pantherdb.org/tempFamilySearch?type=family_name&book=";

	//Change this to the location of pruned panther files that you have saved locally
	private String PATH_LOCAL_PRUNED_TREES = "src/main/resources/panther/pruned_panther_files/";
	private String PATH_FAMILY_LIST = "src/main/resources/panther/familyList.json";
	private String PATH_FAMILY_NAMES_LIST = "src/main/resources/panther/familyNamesList.json";
	private String PATH_HT_LIST = "src/main/resources/panther/familyHTList.csv";
	private String PATH_NP_LIST = "src/main/resources/panther/familyNPList.csv";

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

		List<String> allFamilies = flJson.getFamilyList();
		return allFamilies;
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
		int commitCount = 100;
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

	public void atomicUpdateSolr() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		int plantGenomeCount = 0;
		int commitedCount = 0;
		int errorCount = 0;
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			boolean hasPlantGenome = new PantherBookXmlToJson().hasPlantGenome(origPantherData);
			if(hasPlantGenome) {
				SolrInputDocument sdoc = new SolrInputDocument();
				sdoc.addField("id", pantherFamilyList.get(i));

				List<String> speciation_events = new PantherBookXmlToJson().getFieldValue(origPantherData);
				if (speciation_events != null) {
					Map<String, List<String>> partialUpdate = new HashMap<>();
					partialUpdate.put("set", speciation_events);
					sdoc.addField("species_list", partialUpdate);
					solr.add(sdoc);
					solr.commit();
					commitedCount++;
				} else {
					errorCount++;
				}
			} else {
				plantGenomeCount++;
			}
		}
	}

	//Analyze panther trees and find out all trees with Hori_Transfer node in it. Write the ids to a csv
	public void analyzePantherTrees() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();

		File csvFile = new File(PATH_HT_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		CSVWriter writer = new CSVWriter(outputfile);
		String[] header = {"PantherID"};
		writer.writeNext(header);
		int total = 0;
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			//Is Horizontal Transfer
			boolean isHorizTransfer = new PantherBookXmlToJson().isHoriz_Transfer(origPantherData);
			if(isHorizTransfer) {
				String[] data1 = {pantherFamilyList.get(i)};
				writer.writeNext(data1);
			}
		}
		writer.close();
	}

	//Delete panther trees from solr which don't have any plant genes in it. Save all the deleted ids into a csv
	public void deleteTreesWithoutPlantGenes() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();

		File csvFile = new File(PATH_NP_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		CSVWriter writer = new CSVWriter(outputfile);
		String[] header = {"Deleted Ids"};
		writer.writeNext(header);
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			//Has plant genome
			boolean hasPlantGenome = new PantherBookXmlToJson().hasPlantGenome(origPantherData);
			if(!hasPlantGenome) {
				String[] data1 = {pantherFamilyList.get(i)};
				solr.deleteById(pantherFamilyList.get(i));
				System.out.println("deleted " + pantherFamilyList.get(i));
				writer.writeNext(data1);
			}
		}
		solr.commit();
		System.out.println("Solr commit done");
		writer.close();
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

	public void saveAndCommitToSolr(List<PantherData> pantherList) throws Exception {
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
//		Map<String, String> idToFamilyNames = etl.getLocalPantherFamilyNamesList();
//		etl.indexSolrDB(idToFamilyNames);

		//Update a single fild in solr without reindex
//		etl.atomicUpdateSolr();

//		etl.analyzePantherTrees();

		//Delete panther trees without plant genes.
//		etl.deleteTreesWithoutPlantGenes();
	}

}
