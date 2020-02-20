package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.opencsv.CSVWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONObject;

import org.tair.module.*;
import org.tair.util.Util;

import java.io.*;
import java.util.*;

public class PantherETLPipeline {

	private String URL_SOLR = "http://localhost:8983/solr/panther";
//		String URL_SOLR = "http://54.68.67.235:8983/solr/panther";
	private String URL_PTHR_FAMILY_LIST = "http://pantherdb.org/tempFamilySearch?type=family_list&taxonFltr=13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,559292,284812";
	private String URL_PTHR_FAMILY_NAME = "http://pantherdb.org/tempFamilySearch?type=family_name&book=";
	//Change resources base to your local resources panther folder
	private String RESOURCES_BASE = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes_data/PantherPipelineResources/panther15/panther";

	//Change this to the location of where you have saved panther data
	private String PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList.json";
	private String PATH_FAMILY_NAMES_LIST = RESOURCES_BASE + "/familyNamesList.json";
	private String PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
	private String PATH_LOCAL_MSA_DATA = RESOURCES_BASE +"/panther/msa_files/";
	private String PATH_LOCAL_BOOKINFO_JSON = RESOURCES_BASE + "/panther_jsons/";

	private String PATH_HT_LIST = RESOURCES_BASE + "/familyHTList.csv";
	private String PATH_NP_LIST = RESOURCES_BASE + "/familyNoPlantsList.csv";
	private String PATH_EMPTY_LIST = RESOURCES_BASE + "/familyEmptyWhileIndexingList.csv";
	// log family that has large msa data
	private String PATH_LARGE_MSA_LIST = RESOURCES_BASE + "/largeMsaFamilyList.csv";
	// log family that has invalid msa data
	private String PATH_INVALID_MSA_LIST = RESOURCES_BASE + "/invalidMsaFamilyList.csv";

	PantherServerWrapper pantherServer = new PantherServerWrapper();
	PantherS3Wrapper pantherS3Server = new PantherS3Wrapper();

	SolrClient solr = new HttpSolrClient.Builder(URL_SOLR).build();

	ObjectMapper mapper = new ObjectMapper();
	ObjectWriter ow = new ObjectMapper().writer();
	int committedCount = 0;

	List<String> noPlantsIdList = new ArrayList<>();

	public void atomicUpdateSolr() throws Exception {
		//Example code to update just one field
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

	private void logErrorFamilyList(String pantherId, File outputFile, String errorData) throws IOException {
		FileWriter fileWriter = new FileWriter(outputFile, true);
		CSVWriter csvWriter = new CSVWriter(fileWriter);
		String[] line = {pantherId, errorData};
		csvWriter.writeNext(line);
		csvWriter.close();
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

	//############################################## Panther 15 ########################################################
	//Locally Save panther family list for all panther ids.
	public void updateOrSaveFamilyList_Json() throws Exception{
		String familyListJson = pantherServer.getPantherFamilyListFromServer();
		saveJsonStringAsFile(familyListJson, PATH_FAMILY_LIST);
		System.out.println("Saved family list to " + PATH_FAMILY_LIST);
	}

	//Locally Save panther family names for all panther ids.
	public void updateOrSavePantherFamilyNames_Json() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		JSONObject jo = new JSONObject();
		Collection<JSONObject> items = new ArrayList<JSONObject>();
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			JSONObject item = new JSONObject();
			item.put("pantherId", pantherFamilyList.get(i));
			item.put("familyName", pantherServer.getFamilyNameFromServer(pantherFamilyList.get(i)));
			items.add(item);
			if(items.size() % 100 == 0) {
				System.out.println(items.size() + " items added");
			}
		}
		jo.put("familyNames", new JSONArray(items));
		saveJsonStringAsFile(jo.toString(), PATH_FAMILY_NAMES_LIST);
		System.out.println("Saved family names list to " + PATH_FAMILY_NAMES_LIST);
	}

	//Locally Save panther family trees for all panther ids.
	public void updateOrSavePantherTrees_Json() throws Exception {
		List<String> familyList = getLocalPantherFamilyList();
		//#14712
		int startingIdx = 0;
		for(int i = startingIdx; i < familyList.size(); i++) {
			String familyId = familyList.get(i);
			savePantherTreeLocallyById(familyId, i);
		}
	}

	//Save/update the latest msa data from panther server Locally and on S3 server
	public void updateOrSaveMSAData() throws Exception {
		List<String> familyList = getLocalPantherFamilyList();
		loadNoPlantsIDList();
		for(int i = 0; i < familyList.size(); i++) {
			if (noPlantsIdList.contains(familyList.get(i))) {
				System.out.println("ID " + familyList.get(i) + " has been deleted");
				continue;
			}
			String msaData = pantherServer.readMsaByIdFromServer(familyList.get(i));
			//Save json string as local file
			String fileName = familyList.get(i) + ".json";
			String json_filepath = PATH_LOCAL_MSA_DATA + "/" + fileName;

			JSONObject jo = new JSONObject();
			Collection<JSONObject> items = new ArrayList<JSONObject>();
			JSONObject item = new JSONObject();
			item.put("id", familyList.get(i));
			item.put("msa_data", msaData);
			items.add(item);
			jo.put("familyNames", new JSONArray(items));
			String bucketName = "test-phg-msadata";
			pantherS3Server.uploadJsonToS3(bucketName, fileName, jo.toString());
			saveJsonStringAsFile(jo.toString(), json_filepath);
			System.out.println("IDx saved " + i);
		}
	}

	//Locally Save trees from panther server for given id
	public void savePantherTreeLocallyById(String familyId, int idx) throws Exception {
		long startTime = System.nanoTime();
		PantherData pantherData = pantherServer.readPantherTreeById(familyId);
		String filePath = PATH_LOCAL_PRUNED_TREES + familyId+ ".json";
		try {
			saveJavaObjectAsFile(pantherData, filePath);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime);
			System.out.println("Saved: "+pantherData.getId() + " idx: " + idx + " duration " + duration/1000000 + "ms");
		} catch(Exception e) {
			System.out.println("Error in saving "+pantherData.getId());
		}
	}

	//Reindex Solr DB with modified panther data
	public void indexSolrDB(boolean saveToS3) throws Exception {
		Map<String, String> idToFamilyNames = getLocalPantherFamilyNamesList();
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		List<PantherData> pantherList = new ArrayList<>();
		loadNoPlantsIDList();
		int commitCount = 100;

		//Log empty files inside a csv (will overwrite any content inside)
		File csvFile = new File(PATH_EMPTY_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		CSVWriter writer = new CSVWriter(outputfile);
		String[] header = {"Empty Ids panther trees"};
		writer.writeNext(header);

		for(int i = 0; i < pantherFamilyList.size(); i++) {
			if(noPlantsIdList.contains(pantherFamilyList.get(i))) {
				System.out.println("ID " + pantherFamilyList.get(i) + " has been deleted");
				continue;
			}
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i));
			String familyName = idToFamilyNames.get(pantherFamilyList.get(i));
			System.out.println("ID " + pantherFamilyList.get(i) + " familyName "+ familyName);
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);

			//Some panther trees might be empty after pruning, so we should not add it to solr.
			// We save the empty panther tree ids inside "emptyPantherIds"
			if(modiPantherData != null) {
				pantherList.add(modiPantherData);
			} else {
				System.out.println("Empty panther tree found "+ origPantherData.getId());
				String[] data1 = {pantherFamilyList.get(i)};
				writer.writeNext(data1);
				continue;
			}
			System.out.println(modiPantherData.getId() + " idx: " + i + " size: " + modiPantherData.getJsonString().length());
			saveAndCommitToSolr(pantherList);
			pantherList.clear();

			//Save json string as local file
			if(saveToS3) {
				String fileName = pantherFamilyList.get(i) + ".json";
				String json_filepath = PATH_LOCAL_BOOKINFO_JSON + "/" + fileName;
				String jsonStr = modiPantherData.getJsonString();

				saveJsonStringAsFile(jsonStr, json_filepath);

				String BUCKET_NAME = "test-swapp-bucket";
				pantherS3Server.uploadJsonToS3(BUCKET_NAME, fileName, jsonStr);
			}
		}
		writer.close();
//		saveAndCommitToSolr(pantherList);
//		pantherList.clear();
	}

	//Delete panther trees from local which don't have any plant genes in it. Save all the deleted ids into a csv
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
				String filePath = PATH_LOCAL_PRUNED_TREES + pantherFamilyList.get(i) + ".json";
//				System.out.println(filePath);
				File fileToDelete = new File(filePath);
				if(fileToDelete.delete()) {
					System.out.println("deleted " + pantherFamilyList.get(i));
				} else {
					System.out.println("Failed to delete " + pantherFamilyList.get(i));
				}
				writer.writeNext(data1);
			}
		}
		writer.close();
	}

	//Older code for setting msa inside solr (now its saved in S3 bucket)
	private void updateMsaData_old() throws Exception {
		List<String> pantherFamilyList = getLocalPantherFamilyList();
		PantherMsaXmlToJson pantherMsaXmlToJson = new PantherMsaXmlToJson();
		File largeMsaFile = new File(PATH_LARGE_MSA_LIST);
		FileWriter largeMsaFileWriter = new FileWriter(largeMsaFile);
		CSVWriter largeMsaCSVWriter = new CSVWriter(largeMsaFileWriter);
		File invalidMsaFile = new File(PATH_INVALID_MSA_LIST);
		FileWriter invalidMsaFileWriter = new FileWriter(invalidMsaFile);
		CSVWriter invalidMsaCSVWriter = new CSVWriter(invalidMsaFileWriter);
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			String pantherId = pantherFamilyList.get(i);
			System.out.println("Processing : "+pantherId+" idx: "+i);
			PantherData origPantherData = readPantherBooksFromLocal(pantherId);
			boolean hasPlantGenome = new PantherBookXmlToJson().hasPlantGenome(origPantherData);
			if(hasPlantGenome) {
				SolrInputDocument sdoc = new SolrInputDocument();
				sdoc.addField("id", pantherId);

				MsaData msaData = pantherMsaXmlToJson.readMsaById(pantherId);
				List<String> sequence_info;
				try {
					sequence_info = msaData.getSearch().getSequence_list().getSequence_info();
				}catch (NullPointerException e){
					logErrorFamilyList(pantherId, invalidMsaFile, ow.writeValueAsString(msaData));
					continue;
				}

				if (sequence_info.size() == 0) {
					continue;
				}else if (sequence_info.size()>30000) {
					logErrorFamilyList(pantherId, largeMsaFile, Integer.toString(sequence_info.size()));
					continue;
				}else{
					Map<String, String> partialDelete = new HashMap<>();
					partialDelete.put("removeregex", ".*");
					sdoc.setField("msa_data", partialDelete);
					solr.add(sdoc);
					solr.commit();
//					System.out.println("Cleared msa_data for: " + pantherId);
					Map<String, List<String>> partialUpdate = new HashMap<>();
					int ChunkSize = 10000;
					for (int j=0; j<sequence_info.size(); j += ChunkSize){
						int end;
						if (j+ChunkSize < sequence_info.size()){
							end = j+ChunkSize;
						}else{
							end = sequence_info.size();
						}
						partialUpdate.put("add", sequence_info.subList(j,end));
						sdoc.setField("msa_data", partialUpdate);
						try {
							solr.add(sdoc);
							solr.commit();
						}catch (SolrServerException e){
							e.printStackTrace();
						}
//						System.out.println("Added msa data: " + j + "-" + end);
					}
					System.out.println("Updated msa data for: "+pantherId);
				}
			} else {
				continue;
			}
		}
		invalidMsaCSVWriter.close();
		largeMsaCSVWriter.close();
	}

	//Set "uniprot_ids_count" field for panther solr
	public void setUniprotIdsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(8900);
		sq.setFields("id, uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = solr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " "+ treeId); //debugging visualization
			if(treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("uniprot_ids_count", partialUpdate);
				solr.add(sdoc);
				solr.commit();
			} else {
				System.out.println("null");
			}
		}
	}

	//Set "go_annotations_count" field for panther solr
	public void setGoAnnotationsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id, go_annotations");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = solr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " "+ treeId); //debugging visualization
			if(treeIdResponse.getResults().get(i).getFieldValues("go_annotations") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("go_annotations_count", partialUpdate);
				solr.add(sdoc);
				solr.commit();
			} else {
				System.out.println("null");
			}
		}
	}

	//Analyze panther trees and find out all trees with Hori_Transfer node in it. Write the ids to a csv
	public void analyzePantherTrees() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = solr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound "+ totalDocsFound);

		File csvFile = new File(PATH_HT_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		String[] header = {"PantherID"};
		CSVWriter writer = new CSVWriter(outputfile);
		writer.writeNext(header);

		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			PantherData origPantherData = readPantherBooksFromLocal(treeId);
			//Is Horizontal Transfer
			boolean isHorizTransfer = new PantherBookXmlToJson().isHoriz_Transfer(origPantherData);
			if(isHorizTransfer) {
				String[] data1 = {treeId};
				writer.writeNext(data1);
			}
		}
		writer.close();
	}
	//################## utils

	//Get Panther Family List from local file if it exists
	public List<String> getLocalPantherFamilyList() throws Exception {
		InputStream input = new FileInputStream(PATH_FAMILY_LIST);

		String data = mapper.readValue(input, String.class);

		PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyList.class);

		List<String> allFamilies = flJson.getFamilyList();
		System.out.format("Found %d families", allFamilies.size());
		return allFamilies;
	}

	//Save Json String as File
	public void saveJsonStringAsFile(String jsonString, String filepath) throws Exception {
		File jsonFile = new File(filepath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, jsonString);
	}

	//Save Java Object as Json File
	public void saveJavaObjectAsFile(Object jsonObj, String filepath) throws Exception {
		File jsonFile = new File(filepath);
		jsonFile.setExecutable(true);
		jsonFile.setReadable(true);
		jsonFile.setWritable(true);
		jsonFile.createNewFile();
		mapper.writeValue(jsonFile, jsonObj);
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

	//Read panther book_info json from local
	public PantherData readPantherBooksFromLocal(String id) throws Exception {
		String filePath = PATH_LOCAL_PRUNED_TREES + id + ".json";
		InputStream input = new FileInputStream(filePath);

		PantherData data = mapper.readValue(input, PantherData.class);
		return data;
	}

	//Loads the No Plants IDs which are used to check if this file has been deleted from local (due to no plants)
	// Make sure the PATH_NP_LIST contains a csv with the updated deleted ids, which is obtained by performing "deleteTreesWithoutPlantGenes()"
	// after panther trees are saved locally.
	public void loadNoPlantsIDList() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(PATH_NP_LIST));
		try {
			String line = br.readLine();
			while (line != null) {
				line = br.readLine();
				if(line != null) {
					line = line.replaceAll("^\"|\"$", "");
//					System.out.println(line);
					noPlantsIdList.add(line);
				}
			}
		} finally {
			br.close();
		}
	}

	public static void main(String args[]) throws Exception {
		long startTime = System.nanoTime();

		PantherETLPipeline etl = new PantherETLPipeline();

		//Pipeline for Every New Panther release
		// 1. Update the local family list json file from server
//		etl.updateOrSaveFamilyList_Json();
		// 2. Save the mapping of family ID with family name in json from server
//		etl.updateOrSavePantherFamilyNames_Json();
		// 3. Download all panther family json files from server
//		etl.updateOrSavePantherTrees_Json();
		// 4. Delete panther trees without plant genes.
//		etl.deleteTreesWithoutPlantGenes();
		// 5. Reindex Solr DB based on local panther files and change in solr schema.
		// Set saveToS3 = true, if you want to overwrite s3 book_info files also
		etl.indexSolrDB(false);
		// 6. Save MSA data from server to s3 and local
//		etl.updateOrSaveMSAData();
		// 7. Go to GoAnnotationETLPipeline and update "uniprotdb" on solr with the mapping of uniprot Ids with GO Annotations
		// 8. Go to UpdateGOAnnotations to update/add go annotations field for panther trees loaded using the "uniprot" core on solr.

		//9. Set uniprotIds and GoAnnotations Count on solr
//		etl.setUniprotIdsCount();
//		etl.setGoAnnotationsCount();

		//10. Analyze panther trees
//		etl.analyzePantherTrees();
		//Update a single fild in solr without reindex
//		etl.atomicUpdateSolr();

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " +
				timeElapsed / 1000000);
	}

}
