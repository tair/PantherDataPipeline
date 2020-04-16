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
import org.tair.process.uniprotdb.GOAnnotationETLPipeline;
import org.tair.process.uniprotdb.UpdateGOAnnotations;
import org.tair.util.Util;

import java.io.*;
import java.util.*;

public class PantherETLPipeline {

	private String URL_SOLR = "http://localhost:8983/solr/panther";
//		String URL_SOLR = "http://54.68.67.235:8983/solr/panther";
	//Change resources base to your local resources panther folder
	private String RESOURCES_BASE = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes_data/PantherPipelineResources/panther15-newApi/panther";

	//Panther server api
	private String PANTHER_FL_URL = "http://pantherdb.org/services/oai/pantherdb/supportedpantherfamilies";
	//Change this to the location of where you have saved panther data
	private String PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList/";
	private String PATH_FAMILY_NAMES_LIST = RESOURCES_BASE + "/familyNamesList.json";
	private String PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
	private String PATH_LOCAL_MSA_DATA = RESOURCES_BASE +"/msa_jsons/";
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
		List<FamilyNode> pantherFamilyList = getLocalPantherFamilyList(1);
		int plantGenomeCount = 0;
		int commitedCount = 0;
		int errorCount = 0;
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i).getFamily_id());
			if(origPantherData != null) {
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
	    int totalNumberOfFamilies = 15702;
	    int si = 1;
        int ei = 1000;
        while(ei < totalNumberOfFamilies) {
            String url = PANTHER_FL_URL + "?startIndex="+si;
            String familyListJson = pantherServer.getPantherFamilyListFromServer(url);
            String filename = PATH_FAMILY_LIST + "familyList_" + si +".json";
            Util.saveJsonStringAsFile(familyListJson, filename);
            String jsonText = Util.loadJsonStringFromFile(filename);
            JSONObject jsonObj = new JSONObject(jsonText);
            int endIndex = jsonObj.getJSONObject("search").getInt("end_index");
            if(si == 0) {
                totalNumberOfFamilies = jsonObj.getJSONObject("search").getInt("number_of_families");
            }
            System.out.println("Saved family list as " + "familyList_" + si +".json");
            System.out.println("endIndex "+ endIndex);
            ei = endIndex;
            si = ei+1;
        }
	}

	//Locally Save panther family names for all panther ids.
	public void updateOrSavePantherFamilyNames_Json() throws Exception {
		List<FamilyNode> pantherFamilyList = getLocalPantherFamilyList(1);
		JSONObject jo = new JSONObject();
		Collection<JSONObject> items = new ArrayList<JSONObject>();
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			JSONObject item = new JSONObject();
			item.put("pantherId", pantherFamilyList.get(i));
			item.put("familyName", pantherServer.getFamilyNameFromServer(pantherFamilyList.get(i).getFamily_id()));
			items.add(item);
			if(items.size() % 100 == 0) {
				System.out.println(items.size() + " items added");
			}
		}
		jo.put("familyNames", new JSONArray(items));
		Util.saveJsonStringAsFile(jo.toString(), PATH_FAMILY_NAMES_LIST);
		System.out.println("Saved family names list to " + PATH_FAMILY_NAMES_LIST);
	}

	//Locally Save panther family trees for all panther ids.
	public void updateOrSavePantherTrees_Json() throws Exception {
	    int si = 14001;
		while(si < 16001) {
            List<FamilyNode> familyListBatch = getLocalPantherFamilyList(si);
            System.out.println("Saving families from Family List " + si);
            int ei = familyListBatch.size();
            for(int i = 0; i < ei; i++) {
                String familyId = familyListBatch.get(i).getFamily_id();
                savePantherTreeLocallyById(familyId, i);
            }
            si += 1000;
        }
	}

	//Save/update the latest msa data from panther server Locally and on S3 server
	public void updateOrSaveMSAData() throws Exception {
		int si = 3001;
		while(si < 16001) {
			System.out.println("start Idx "+si);
			List<FamilyNode> familyList = getLocalPantherFamilyList(si);
			for (int i = 0; i < familyList.size(); i++) {
				String msaData = pantherServer.readMsaByIdFromServer(familyList.get(i).getFamily_id());
				//Save json string as local file
				String fileName = familyList.get(i).getFamily_id() + ".json";
				String json_filepath = PATH_LOCAL_MSA_DATA + "/" + fileName;

				JSONObject jo = new JSONObject();
				Collection<JSONObject> items = new ArrayList<JSONObject>();
				JSONObject item = new JSONObject();
				item.put("id", familyList.get(i));
				item.put("msa_data", msaData);
				items.add(item);
				jo.put("familyNames", new JSONArray(items));
				String bucketName = "phg-panther-msa-data";
				pantherS3Server.uploadJsonToS3(bucketName, fileName, jo.toString());
				Util.saveJsonStringAsFile(jo.toString(), json_filepath);
				if(i%20 == 0) {
					System.out.println("IDx saved " + i);
				}
			}
			si = si + 1000;
		}
	}

	//Locally Save trees from panther server for given id
	public void savePantherTreeLocallyById(String familyId, int idx) throws Exception {
		long startTime = System.nanoTime();
		String origPantherData = pantherServer.readPantherTreeById(familyId);
		if(origPantherData.isEmpty()) {
            System.out.println("Json is empty "+ familyId);
            return;
        }
//		System.out.println(pantherData);
		String filePath = PATH_LOCAL_PRUNED_TREES + familyId+ ".json";
		try {
		    PantherData pantherData = new PantherData();
		    pantherData.setId(familyId);
            pantherData.setJsonString(origPantherData);
            saveJavaObjectAsFile(pantherData, filePath);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime);
			System.out.println("Saved: "+familyId + " idx: " + idx + " duration " + duration/1000000 + "ms");
		} catch(Exception e) {
			System.out.println("Error in saving "+familyId);
		}
	}

    public void indexSolrDBTest() throws Exception {
		PantherData origPantherData = readPantherBooksFromLocal("PTHR10003");

		PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, "test");
		System.out.println(modiPantherData.getPersistent_ids());
	}

	//Reindex Solr DB with modified panther data
	public void indexSolrDB(boolean saveToS3) throws Exception {
		int si = 1;
		//Log empty files inside a csv (will overwrite any content inside)
		File csvFile = new File(PATH_EMPTY_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		CSVWriter writer = new CSVWriter(outputfile);
		String[] header = {"Empty Ids panther trees"};
		writer.writeNext(header);

		while(si < 16001) {
			List<FamilyNode> pantherFamilyList = getLocalPantherFamilyList(si);
			List<PantherData> pantherList = new ArrayList<>();
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i).getFamily_id());
				if (origPantherData != null) {
					String familyName = pantherFamilyList.get(i).getFamily_name();
//				System.out.println("ID " + pantherFamilyList.get(i) + " familyName " + familyName);
					PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);

					//Some panther trees might be empty after pruning, so we should not add it to solr.
					// We save the empty panther tree ids inside "emptyPantherIds"
					if (modiPantherData != null) {
						pantherList.add(modiPantherData);
					} else {
						System.out.println("Empty panther tree found " + origPantherData.getId());
						String[] data1 = {pantherFamilyList.get(i).getFamily_id()};
						writer.writeNext(data1);
						continue;
					}
					System.out.println(modiPantherData.getId() + " idx: " + i + " size: " + modiPantherData.getJsonString().length());
					saveAndCommitToSolr(pantherList);
					pantherList.clear();

					//Save json string as local file
					if (saveToS3) {
						String fileName = pantherFamilyList.get(i).getFamily_id() + ".json";
						String json_filepath = PATH_LOCAL_BOOKINFO_JSON + "/" + fileName;
						String jsonStr = modiPantherData.getJsonString();

						Util.saveJsonStringAsFile(jsonStr, json_filepath);

						String BUCKET_NAME = "phg-panther-data";
						pantherS3Server.uploadJsonToS3(BUCKET_NAME, fileName, jsonStr);
					}
				}
			}
			si = si+1000;
		}
		writer.close();
	}

	//Delete panther trees from local which don't have any plant genes in it. Save all the deleted ids into a csv
	public void deleteTreesWithoutPlantGenes() throws Exception {
		int si = 1;
		File csvFile = new File(PATH_NP_LIST);
		FileWriter outputfile = new FileWriter(csvFile);
		CSVWriter writer = new CSVWriter(outputfile);
		String[] header = {"Deleted Ids"};
		writer.writeNext(header);
		while(si < 16001) {
			System.out.println("index "+ si);
			List<FamilyNode> pantherFamilyList = getLocalPantherFamilyList(si);
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				PantherData origPantherData = readPantherBooksFromLocal(pantherFamilyList.get(i).getFamily_id());
				if (origPantherData != null) {
					//Has plant genome
					boolean hasPlantGenome = new PantherBookXmlToJson().hasPlantGenome(origPantherData);
					if (!hasPlantGenome) {
						String[] data1 = {pantherFamilyList.get(i).getFamily_id()};
						String filePath = PATH_LOCAL_PRUNED_TREES + pantherFamilyList.get(i).getFamily_id() + ".json";
						String filePathToMove = PATH_LOCAL_PRUNED_TREES + "Deleted/" + pantherFamilyList.get(i).getFamily_id() + ".json";
						File fileToDelete = new File(filePath);
						fileToDelete.renameTo(new File(filePathToMove));
						System.out.println("deleted " + pantherFamilyList.get(i).getFamily_id());
						writer.writeNext(data1);
					}
				} else {
					String[] data1 = {pantherFamilyList.get(i).getFamily_id()};
					writer.writeNext(data1);
				}
			}
			si = si+1000;
		}
		writer.close();
	}

	//Older code for setting msa inside solr (now its saved in S3 bucket)
	private void updateMsaData_old() throws Exception {
		List<FamilyNode> pantherFamilyList = getLocalPantherFamilyList(1);
		PantherMsaXmlToJson pantherMsaXmlToJson = new PantherMsaXmlToJson();
		File largeMsaFile = new File(PATH_LARGE_MSA_LIST);
		FileWriter largeMsaFileWriter = new FileWriter(largeMsaFile);
		CSVWriter largeMsaCSVWriter = new CSVWriter(largeMsaFileWriter);
		File invalidMsaFile = new File(PATH_INVALID_MSA_LIST);
		FileWriter invalidMsaFileWriter = new FileWriter(invalidMsaFile);
		CSVWriter invalidMsaCSVWriter = new CSVWriter(invalidMsaFileWriter);
		for(int i = 0; i < pantherFamilyList.size(); i++) {
			String pantherId = pantherFamilyList.get(i).getFamily_id();
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

	//Get Panther Family List from local files if it exists. With new api the family list is saved as batches of 1000 ids.
	public List<FamilyNode> getLocalPantherFamilyList(int start_index) throws Exception {
	    String filename = PATH_FAMILY_LIST + "familyList_"+start_index+".json";
		InputStream input = new FileInputStream(filename);

		String data = mapper.readValue(input, String.class);

		PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyList.class);

		List<FamilyNode> allFamilies = flJson.getFamilyNodes();
		System.out.format("Found %d families", allFamilies.size());
		return allFamilies;
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
		InputStream input = null;
		try {
			input = new FileInputStream(filePath);

		} catch(Exception e) {
			return null;
		}

        PantherData data = mapper.readValue(input, PantherData.class);
        return data;
	}

    public PantherData readPantherBooksFromLocal(FamilyNode node) throws Exception {
        String filePath = PATH_LOCAL_PRUNED_TREES + node.getFamily_id() + ".json";
        InputStream input = new FileInputStream(filePath);

        PantherData data = mapper.readValue(input, PantherData.class);
        data.setFamily_name(node.getFamily_name());
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
		// 1. Update the local family list json file from server.
		// In the new panther api, family id has family name already mapped
//		etl.updateOrSaveFamilyList_Json();
		// 3. Download all panther family json files from server
//		etl.updateOrSavePantherTrees_Json();
		// 4. Delete panther trees without plant genes.
//		etl.deleteTreesWithoutPlantGenes();
		// 5. Reindex Solr DB based on local panther files and change in solr schema.
		// Set saveToS3 = true, if you want to overwrite s3 book_info files also
//		etl.indexSolrDB(true);
//        etl.indexSolrDBTest();
		// 6. Save MSA data from server to s3 and local
//		etl.updateOrSaveMSAData();
		// 7. update "uniprotdb" on solr with the mapping of uniprot Ids with GO Annotations
		// important: if the url of gaf file or obo file changes, we need to update them in applications.properties file, otherwise it may not reflect the correct data;
		// if the format of gaf file or obo file has been changed, we need to change the code accordingly.
//		GOAnnotationETLPipeline goAnnotationETLPipeline = new GOAnnotationETLPipeline();
//		goAnnotationETLPipeline.storeGOAnnotationFromApiToUniprotDb();
//		goAnnotationETLPipeline.updateGOAnnotationFromFileToUniprotDb();
		// 8. update/add go annotations field for panther trees loaded using the "uniprot" core on solr.
		UpdateGOAnnotations UpdateGOAnnotations= new UpdateGOAnnotations();
		UpdateGOAnnotations.updateGOAnnotations();

		//9. Set uniprotIds and GoAnnotations Count on solr
//		etl.setUniprotIdsCount();
//		etl.setGoAnnotationsCount();

		//10. Analyze panther trees
//		etl.analyzePantherTrees();
		//Update a single fild in solr without reindex
//		etl.atomicUpdateSolr();

		//11. Go to pantherToPhyloXmlPipeline Update PhyloXML files locally and on S3

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " +
				timeElapsed / 1000000);
	}

}
