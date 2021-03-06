package org.tair.process.panther;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;

import org.tair.module.*;
import org.tair.module.panther.Annotation;
import org.tair.process.PantherBookXmlToJson;
import org.tair.process.paint.GOAnnotationPaintETLPipeline;

import java.util.*;

public class PantherETLPipeline {

	PantherServerWrapper pantherServer = new PantherServerWrapper();
	PantherLocalWrapper pantherLocal = new PantherLocalWrapper();
	PhylogenesServerWrapper pgServer = new PhylogenesServerWrapper();
	int batchLimit = 16001;

	//############################################## Panther 15 ########################################################
	public void storePantherFilesLocally() throws Exception {
	 /**
	  * 1. Update the local family list json file from panther server.
	  * 2. Download all panther family json files from panther server to local folder
	  * 3. Delete panther trees without plant genes.
	  * 4. Download all MSA json files from panther server to local folder
	  */
//		updateOrSaveFamilyList_Json();
//		updateOrSavePantherTrees_Json();
//		deleteTreesWithoutPlantGenes();
//		updateOrSaveMSAData();
	}

	public void uploadToServer() throws Exception {
		/**
		 * 6. Reindex Solr DB based on local panther files and change in solr schema.
		 *
		 */
//		indexSolrDB(false);

		/**
		 * 7. update "uniprotdb" on solr with the mapping of uniprot Ids with GO Annotations
		 // important: if the url of gaf file or obo file changes, we need to update them in applications.properties file, otherwise it may not reflect the correct data;
		 // if the format of gaf file or obo file has been changed, we need to change the code accordingly.
		 */
//		GOAnnotationPaintETLPipeline anno_pipeline = new GOAnnotationPaintETLPipeline();
//		GOAnnotationETLPipeline goAnnotationETLPipeline = new GOAnnotationETLPipeline();
//		goAnnotationETLPipeline.storeGOAnnotationFromApiToUniprotDb();
//		goAnnotationETLPipeline.updateGOAnnotationFromFileToUniprotDb();

		/**
		 * 8. update/add go annotations field for panther trees loaded using the "uniprot" core on solr.
		 */
//		PantherUpdateGOAnnotations PantherUpdateGOAnnotations= new PantherUpdateGOAnnotations();
//		PantherUpdateGOAnnotations.updateGOAnnotations();

		/**
		 * 9. Set uniprotIds and GoAnnotations Count on solr for each tree
		 */
//		pgServer.setUniprotIdsCount();
//		pgServer.setGoAnnotationsCount();

		//10. Analyze panther trees
//		pgServer.analyzePantherTrees();

		//11. Go to pantherToPhyloXmlPipeline Update PhyloXML files locally and on S3
	}

	public void updateSolr_selected() throws Exception {
		String[] sel_ids = new String[]{"PTHR10334"};
		for(int j=0; j<sel_ids.length;j++) {
			indexSingleIdOnSolr(sel_ids[j]);
		}
		PantherUpdateGOAnnotations PantherUpdateGOAnnotations = new PantherUpdateGOAnnotations();
		PantherUpdateGOAnnotations.updateGOAnnotations_selected(sel_ids);
	}

	//Locally Save panther family list for all panther ids.
	public void updateOrSaveFamilyList_Json() throws Exception{
		int totalNumberOfFamilies = 10000;
		int startIdx = 1;
		int endIdx = 1000; //The max batch size for a single panther api call
		System.out.println("PATH_FAMILY_LIST: "+ pantherLocal.getLocalFamiliListPath());
		while(endIdx < totalNumberOfFamilies) {
			String familyListJson = pantherServer.getPantherFamilyListFromServer(startIdx);
			pantherLocal.savePantherFamilyListBatch(familyListJson, startIdx);
			String jsonText = pantherLocal.loadFamilyListBatch(startIdx);
			JSONObject jsonObj = new JSONObject(jsonText);
			endIdx = jsonObj.getJSONObject("search").getInt("end_index");
			if (startIdx == 1) {
				totalNumberOfFamilies = jsonObj.getJSONObject("search").getInt("number_of_families");
				System.out.println("total number of families found " + totalNumberOfFamilies);
			}
			System.out.println("Saved family list as " + "familyList_" + startIdx + ".json, endIndex " + endIdx);
			startIdx = endIdx + 1;
		}
	}

	//Locally Save panther family trees for all panther ids.
	public void updateOrSavePantherTrees_Json() throws Exception {
	    int si = 1;
		System.out.println("PATH_LOCAL_PRUNED_TREES: "+ pantherLocal.getLocalPrunedTreesPath());
		String[] sel_ids = new String[]{"PTHR11875","PTHR33565","PTHR45665","PTHR45687","PTHR46739","PTHR47002"};
		pantherLocal.initLogWriter(1);
		while(si < batchLimit) {
            List<FamilyNode> familyListBatch = pantherLocal.getLocalPantherFamilyList(si);
            int ei = familyListBatch.size();
            for(int i = 0; i < ei; i++) {
                String familyId = familyListBatch.get(i).getFamily_id();
                String familyName = familyListBatch.get(i).getFamily_name();
                for(int j=0; j<sel_ids.length;j++) {
					if(familyId.equals(sel_ids[j])) {
						savePantherTreeLocallyById(familyId, familyName, i);
					}
				}
            }
            si += 1000;
        }
        pantherLocal.closeLogWriter(1);
	}

	//Save/update the latest msa data from panther server Locally and on S3 server
	public void updateOrSaveMSAData() throws Exception {
		int si = 1;
		System.out.println("PATH_LOCAL_MSA_DATA: "+ pantherLocal.getLocalMSAPath());
		String[] sel_ids = new String[]{"PTHR10177"};
		while(si < 16001) {
			System.out.println("start Idx "+si);
			List<FamilyNode> familyList = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < familyList.size(); i++) {
				String familyId = familyList.get(i).getFamily_id();
				for(int j=0; j<sel_ids.length;j++) {
					if (familyId.equals(sel_ids[j])) {
						String msaData = pantherServer.readMsaByIdFromServer(familyId);
						if (msaData.length() < 3) {
							System.out.println("MSA Data is empty " + familyId);
							continue;
						}
						//Save json string as local file
						String msaJson = pantherLocal.saveMSADataAsJsonFile(familyId, msaData);
						String fileName = familyId + ".json";
						pgServer.uploadJsonToPGMsaBucket(fileName, msaJson);
					}
				}
				if(i%20 == 0) {
					System.out.println("IDx saved " + i);
				}
			}
			si = si + 1000;
		}
	}

	//Locally Save trees from panther server for given id
	public void savePantherTreeLocallyById(String familyId, String familyName, int idx) throws Exception {
		long startTime = System.nanoTime();
		String origPantherData = pantherServer.readPantherTreeById(familyId);
		if(origPantherData.isEmpty()) {
            System.out.println("Json is empty "+ familyId);
            pantherLocal.logEmptyId(familyId);
            return;
        }

		try {
		    PantherData pantherData = new PantherData();
		    pantherData.setId(familyId);
		    pantherData.setFamily_name(familyName);
            pantherData.setJsonString(origPantherData);
            pantherLocal.savePantherDataAsJsonFile(pantherData, familyId);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime);
			System.out.println("Saved: "+familyId + " idx: " + idx + " duration " + duration/1000000 + "ms");
		} catch(Exception e) {
			System.out.println("Error in saving "+familyId);
			pantherLocal.logEmptyId(familyId);
		}
	}

	//Delete panther trees from local which don't have any plant genes in it. Save all the deleted ids into a csv
	public void deleteTreesWithoutPlantGenes() throws Exception {
		int si = 1;
		pantherLocal.initLogWriter(0);
		while(si < 20000) {
			System.out.println("index "+ si);
			List<FamilyNode> familyListBatch = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < familyListBatch.size(); i++) {
				String id = familyListBatch.get(i).getFamily_id();
				PantherData origPantherData = pantherLocal.readPantherTreeById(id);
				if (origPantherData != null) {
					//Has plant genome
					boolean hasPlantGenome = new PantherBookXmlToJson().hasPlantGenome(origPantherData);
					if (!hasPlantGenome) {
						pantherLocal.deleteFile(id);
						System.out.println("No plants deleted " + id);
						pantherLocal.logDeletedId(id);
					}
				} else {
					pantherLocal.logDeletedId(id);
				}
			}
			si = si+1000;
		}
		pantherLocal.closeLogWriter(0);
	}

	//Process Pruned Tree Json String by updating values using local mapping files
	public String processPrunedTree(String jsonString) throws Exception {
		PantherData pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		//Mapping to convert tair locus ids to tair gene names
		HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
		Annotation rootNodeAnnotation = pantherData.getSearch().getAnnotation_node();
		rootNodeAnnotation = updatePantherTree(rootNodeAnnotation, tair_locus2id_mapping);
		pantherData.getSearch().setAnnotation_node(rootNodeAnnotation);
		//Convert Java Object to Json String
		ObjectMapper mapper = new ObjectMapper();
		String newJsonStr = mapper.writeValueAsString(pantherData);
		return newJsonStr;
	}

	//Update each node in the tree recursively.
	public Annotation updatePantherTree(Annotation node,HashMap<String, String> mapping) throws Exception {
		//Update Gene_id from mapping
		if(node.getChildren() != null) {
			for(int i=0; i<node.getChildren().getAnnotation_node().size(); i++) {
				Annotation childNode = node.getChildren().getAnnotation_node().get(i);
				if(childNode.getGene_id() != null) {
//					System.out.println(childNode.getGene_id());
					String code = childNode.getGene_id().split(":")[0];
					if(code.equals("TAIR")) {
						String val = childNode.getGene_id().split(":")[1];
						val = val.split("=")[1];
						String updatedGeneId = mapping.get(val);
						childNode.setGene_id(code + ":" + updatedGeneId);
					}
				}
				updatePantherTree(childNode, mapping);
			}
		}
		return node;
	}

	//Update gene names for tair ids.
	public void updateLocusGeneNames() throws Exception {
		HashMap<String, String> mapping = pantherLocal.read_locus2tair_mapping_csv();
		int si = 1;
		while(si < 16001) {
			List<FamilyNode> pantherFamilyList = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				String id = pantherFamilyList.get(i).getFamily_id();

//				if (id.equals("PTHR31989")) {
					System.out.println(id);
					PantherData origPantherData = pantherLocal.readPantherTreeById(id);
					if (origPantherData != null) {
						String familyName = pantherFamilyList.get(i).getFamily_name();
						PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);
						List<String> gene_ids = modiPantherData.getGene_ids();
						for (int j = 0; j < gene_ids.size(); j++) {
							String code = gene_ids.get(j).split(":")[0];
							if(code.equals("TAIR")) {
								String val = gene_ids.get(j).split(":")[1];
								val = val.split("=")[1];
								String updatedGeneId = mapping.get(val);
								System.out.println(val + " _ " + updatedGeneId);
								gene_ids.set(j, code + ":" + updatedGeneId);
							}
						}
						modiPantherData.setGene_ids(gene_ids);
						pgServer.atomicUpdateSolr(id, "gene_ids", modiPantherData.getGene_ids());
						//Update s3 tree
						Annotation rootNodeAnnotation = modiPantherData.getSearch().getAnnotation_node();
						rootNodeAnnotation = updatePantherTree(rootNodeAnnotation, mapping);
						modiPantherData.getSearch().setAnnotation_node(rootNodeAnnotation);
						ObjectMapper mapper = new ObjectMapper();
						String newJsonStr = mapper.writeValueAsString(modiPantherData);
						String filename = id+".json";
//						pgServer.uploadJsonToPGTreeBucket(filename, newJsonStr);
//						pantherLocal.saveSolrIndexedTreeAsFile(id, newJsonStr);
					}
					i++;
					if(i%100 == 0) {
						System.out.println("processed "+ i);
					}
//				}
			}
			si = si+1000;
		}
	}

	//Generate csv files which analyzes panther etl dumps
	public void generate_analyze_dump() throws Exception {
		String filename = "panther_16_dump_feb8.csv";
//		pgServer.analyzePantherDump(filename);
		filename = "panther_16_annos_feb8.csv";
		pgServer.analyzePantherAnnotations2(filename);

	}

	public void indexSingleIdOnSolr(String id) throws Exception {
//		int si = 1;
		PantherData origPantherData = pantherLocal.readPantherTreeById(id);
		System.out.println(origPantherData.getFamily_name());
		List<PantherData> pantherList = new ArrayList<>();
		if (origPantherData != null) {
			String familyName = origPantherData.getFamily_name();
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);
			System.out.println(modiPantherData.getGene_ids().size());
			if (modiPantherData != null) {
				pantherList.add(modiPantherData);
			} else {
				System.out.println("Empty panther tree found " + id);
			}
			pgServer.saveAndCommitToSolr(pantherList);
			pantherList.clear();
			String jsonStr = modiPantherData.getJsonString();
			pantherLocal.saveSolrIndexedTreeAsFile(id, jsonStr);
			String filename = id+".json";
			pgServer.uploadJsonToPGTreeBucket(filename, jsonStr);
		} else {
			System.out.println("File not found (Deleted)" + id);
		}
	}

	//Index solr db with panther trees saved locally, also save the json to S3 bucket
	public void indexSolrDB(boolean saveToS3) throws Exception {
		int si = 1;
		pantherLocal.initLogWriter(1);
		System.out.println("start Idx "+si);
		while(si < 16001) {
			List<FamilyNode> pantherFamilyList = pantherLocal.getLocalPantherFamilyList(si);
			List<PantherData> pantherList = new ArrayList<>();
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				String id = pantherFamilyList.get(i).getFamily_id();
				PantherData origPantherData = pantherLocal.readPantherTreeById(id);
				if (origPantherData != null) {
					String familyName = pantherFamilyList.get(i).getFamily_name();
					PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData, familyName);

					//Some panther trees might be empty after pruning, so we should not add it to solr.
					// We log the empty panther tree ids
					if (modiPantherData != null) {
						pantherList.add(modiPantherData);
					} else {
						System.out.println("Empty panther tree found " + id);
						pantherLocal.logEmptyId(id);
						continue;
					}
					System.out.println(modiPantherData.getId() + " idx: " + i + " size: " + modiPantherData.getJsonString().length());
					pgServer.saveAndCommitToSolr(pantherList);
					pantherList.clear();

					//Save json string as local file
					if (saveToS3) {
						String jsonStr = modiPantherData.getJsonString();
						pantherLocal.saveSolrIndexedTreeAsFile(id, jsonStr);
						String filename = id+".json";
						pgServer.uploadJsonToPGTreeBucket(filename, jsonStr);
					}
				} else {
					System.out.println("File not found (Deleted)" + id);
//					pantherLocal.logEmptyId(id);
					continue;
				}
			}
			si = si+1000;
		}
		pantherLocal.closeLogWriter(0);
	}

	public static void main(String args[]) throws Exception {
		long startTime = System.nanoTime();

		PantherETLPipeline etl = new PantherETLPipeline();
//		etl.storePantherFilesLocally();
//		etl.uploadToServer();
//		etl.updateLocusGeneNames();

//		etl.updateSolr_selected();

		etl.generate_analyze_dump();

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " +
				timeElapsed / 1000000);
	}
}
