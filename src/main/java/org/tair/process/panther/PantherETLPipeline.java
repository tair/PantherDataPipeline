package org.tair.process.panther;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVWriter;
import javafx.util.Pair;
import org.json.JSONObject;

import org.tair.module.*;
import org.tair.module.panther.Annotation;
import org.tair.process.PantherBookXmlToJson;
import org.tair.process.uniprotdb_iba.GO_IBA_Pipeline;
import org.tair.process.uniprotdb_paint.GO_PAINT_Pipeline;
import org.tair.process.pantherToPhyloXmlPipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PantherETLPipeline {

	PantherServerWrapper pantherServer = new PantherServerWrapper();
	PantherLocalWrapper pantherLocal = new PantherLocalWrapper();
	PhylogenesServerWrapper pgServer = new PhylogenesServerWrapper();
	int batchLimit = 20001;

	// ############################################## Panther 15
	// ########################################################
	public void storePantherFilesLocally() throws Exception {
		/**
		 * 1. Update the local family list json file from panther server.
		 * 2. Download all panther family json files from panther server to local folder
		 * 3. Delete panther trees without plant genes.
		 * 4. Download all MSA json files from panther server to local folder
		 */
		// updateOrSaveFamilyList_Json();
		// updateOrSavePantherTrees_Json();
		// deleteTreesWithoutPlantGenes();
		// updateOrSaveMSAData();
		// updateOrSaveGOAnnotations();
	}

	public void uploadToServer() throws Exception {

		/**
		 * 6. Reindex Solr DB based on local panther files and change in solr schema.
		 */
		// indexSolrDB(false);

		// saveLocalMsaToS3();

		/**
		 * 7. update "uniprotdb" and "paint_db" on solr with the mapping of uniprot Ids
		 * with GO Annotations
		 */
		// updateSolrAnnotations();

		/**
		 * 8. update/add go annotations field for panther trees loaded using the
		 * "uniprot" core on solr.
		 */
		// PantherUpdateGOAnnotations PantherUpdateGOAnnotations = new
		// PantherUpdateGOAnnotations();
		// PantherUpdateGOAnnotations.updateGOAnnotations();

		/**
		 * 9. Update Publication Counts on Solr: PHG-329
		 */
		pgServer.updateAllSolrTreePubCounts();

		/**
		 * 10. Set uniprotIds and GoAnnotations Count on solr for each tree
		 */
		// pgServer.setUniprotIdsCount();
		// pgServer.setGoAnnotationsCount();

		// 10. Analyze panther trees
		// pgServer.analyzePantherTrees();
	}

	public void updatePublicationsCount() throws Exception {
		pgServer.updateAllSolrTreePubCounts();
	}

	public void updateSolr_selected() throws Exception {
		String[] sel_ids = new String[] { "PTHR10334" };
		for (int j = 0; j < sel_ids.length; j++) {
			indexSingleIdOnSolr(sel_ids[j]);
		}
		PantherUpdateGOAnnotations PantherUpdateGOAnnotations = new PantherUpdateGOAnnotations();
		PantherUpdateGOAnnotations.updateGOAnnotations_selected(sel_ids);
	}

	// Locally Save panther family list for all panther ids.
	public void updateOrSaveFamilyList_Json() throws Exception {
		int totalNumberOfFamilies = 10000;
		int startIdx = 1;
		int endIdx = 1000; // The max batch size for a single panther api call
		System.out.println("PATH_FAMILY_LIST: " + pantherLocal.getLocalFamiliListPath());
		while (endIdx < totalNumberOfFamilies) {
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

	// Locally Save panther family trees for all panther ids.
	public void updateOrSavePantherTrees_Json() throws Exception {
		int si = 1;
		int nested_si = 0;
		boolean forced = false;

		System.out.println("PATH_LOCAL_PRUNED_TREES: " + pantherLocal.getLocalPrunedTreesPath());
		// String[] sel_ids = new String[] { "PTHR11875", "PTHR33565", "PTHR45665",
		// "PTHR45687", "PTHR46739",
		// "PTHR47002" };
		pantherLocal.initLogWriter(1);
		while (si < batchLimit) {
			List<FamilyNode> familyListBatch = pantherLocal.getLocalPantherFamilyList(si);
			int ei = familyListBatch.size();
			for (int i = nested_si; i < ei; i++) {
				String familyId = familyListBatch.get(i).getFamily_id();
				String familyName = familyListBatch.get(i).getFamily_name();
				if (forced) {
					savePantherTreeLocallyById(familyId, familyName, i);
				}
				if (!forced && !pantherLocal.doesPantherTreeExist(familyId)) {
					System.out.println("Not Exists " + familyId);
					savePantherTreeLocallyById(familyId, familyName, i);
				}
				// for (int j = 0; j < sel_ids.length; j++) {
				// if (familyId.equals(sel_ids[j])) {
				// savePantherTreeLocallyById(familyId, familyName, i);
				// }
				// }
			}
			si += 1000;
		}
		pantherLocal.closeLogWriter(1);
	}

	// Save/update the latest msa data from panther server Locally and on S3 server
	public void updateOrSaveMSAData() throws Exception {
		int si = 1;
		System.out.println("PATH_LOCAL_MSA_DATA: " + pantherLocal.getLocalMSAPath());

		// Only overwrite/save specific MSA IDs. If array is empty, all MSA IDs from the
		// family lists are saved or overwritten.
		String[] selected_ids = new String[] {};
		while (si < 16001) {
			System.out.println("start Idx " + si);
			List<FamilyNode> familyList = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < familyList.size(); i++) {
				String familyId = familyList.get(i).getFamily_id();
				if (selected_ids.length != 0) {
					for (int j = 0; j < selected_ids.length; j++) {
						if (familyId.equals(selected_ids[j])) {
							String msaData = pantherServer.readMsaByIdFromServer(familyId);
							if (msaData.length() < 3) {
								System.out.println("MSA Data is empty " + familyId);
								continue;
							}
							// Save json string as local file
							String msaJson = pantherLocal.saveMSADataAsJsonFile(familyId, msaData);
							String fileName = familyId + ".json";
							pgServer.uploadJsonToPGMsaBucket(fileName, msaJson);
						}
					}
				} else {
					// Save MSAs for all IDs
					String msaData = pantherServer.readMsaByIdFromServer(familyId);
					if (msaData.length() < 3) {
						System.out.println("MSA Data is empty " + familyId);
						continue;
					}
					// // Save json string as local file
					// String msaJson = pantherLocal.saveMSADataAsJsonFile(familyId, msaData);
					// String fileName = familyId + ".json";
					// pgServer.uploadJsonToPGMsaBucket(fileName, msaJson);
				}

				if (i % 20 == 0) {
					// System.out.println("MSA saved " + i);
				}
			}
			si = si + 1000;
		}
	}

	public void saveLocalMsaToS3() throws Exception {
		int si = 1;
		System.out.println("PATH_LOCAL_MSA_DATA: " + pantherLocal.getLocalMSAPath());
		System.out.println("MSA_S3_BUCKET: " + pgServer.PG_MSA_BUCKET_NAME);

		int count_saved = 0;
		while (si < 16001) {
			List<FamilyNode> familyList = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < familyList.size(); i++) {
				String familyId = familyList.get(i).getFamily_id();
				if (pantherLocal.doesPantherTreeExist(familyId)) {
					System.out.println(count_saved + " Saving " + familyId);
					count_saved++;
				}

				String msaJson = pantherLocal.getMSAJsonFile(familyId);
				// System.out.println(msaJson);
				String fileName = familyId + ".json";
				pgServer.uploadJsonToPGMsaBucket(fileName, msaJson);
			}
			si = si + 1000;
		}
	}

	// Locally Save trees from panther server for given id
	public void savePantherTreeLocallyById(String familyId, String familyName, int idx) throws Exception {
		long startTime = System.nanoTime();
		String origPantherData = pantherServer.readPantherTreeById(familyId);
		if (origPantherData.isEmpty()) {
			System.out.println("Json is empty " + familyId);
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
			System.out.println("Saved: " + familyId + " idx: " + idx + " duration " + duration / 1000000 + "ms");
		} catch (Exception e) {
			System.out.println("Error in saving " + familyId);
			pantherLocal.logEmptyId(familyId);
		}
	}

	// Locally Save Go Annotation Files
	public void updateOrSaveGOAnnotations() {
		// 1. Download GO IBA Annotation files
		downloadIbaAnnotations();
		// 2. Download GO PAINT Annotation files
		downloadPaintAnnotations();
	}

	// Latest Download: 03.15.2022 from
	// ftp://ftp.pantherdb.org/downloads/paint/presubmission
	// (Panther 17)
	private void downloadIbaAnnotations() {
		GO_IBA_Pipeline iba_pipeline = new GO_IBA_Pipeline();
		try {
			iba_pipeline.downloadIBAFilesLocally();
		} catch (Exception e) {
			System.out.println("Error while downloading IBA files locally!");
			e.printStackTrace();
		}
	}

	// Latest Download: 03.15.2022 from
	// ftp://ftp.pantherdb.org/downloads/paint/17.0/2022-03-10/Pthr_GO_17.0.tsv.tar.gz
	// (Panther 17)
	private void downloadPaintAnnotations() {
		GO_PAINT_Pipeline paint_pipe = new GO_PAINT_Pipeline();
		try {
			paint_pipe.downloadPAINTFilesLocally();
		} catch (Exception e) {
			System.out.println("Error while downloading IBA files locally!");
			e.printStackTrace();
		}
	}

	public void updateSolrAnnotations() {
		/**
		 * 1. Update "uniprot_db" on solr with the mapping of uniprot Ids with IBA GAF
		 * GO Annotations
		 **/
		// updateIbaSolr();

		/**
		 * 2. Update "paint_db" on solr with the mapping of uniprot Ids with PAINT GO
		 * Annotations
		 **/
		// updatePaintSolr();

	}

	/**
	 * Latest Update:03.15.2022
	 * totalLines 3879262, Num Docs:2620612
	 * Execution time in milliseconds:837011
	 **/
	private void updateIbaSolr() {
		GO_IBA_Pipeline iba_pipeline = new GO_IBA_Pipeline();
		try {
			// WARNING: setting "clearSolr" flag to true will clear the older "uniprot_db"
			// collection on solr, so make sure to have backup
			iba_pipeline.updateIBAGOFromLocalToSolr(true);
		} catch (Exception e) {
			System.out.println("Error while updating solr uniprot_db annotations!");
			e.printStackTrace();
		}
	}

	/**
	 * Latest Update:03.15.2022
	 **/
	private void updatePaintSolr() {
		GO_PAINT_Pipeline paint_pipeline = new GO_PAINT_Pipeline();
		try {
			// WARNING: setting "clearSolr" flag to true will clear the older "uniprot_db"
			// collection on solr, so make sure to have backup
			paint_pipeline.updatePAINTGOFromLocalToSolr(false);
		} catch (Exception e) {
			System.out.println("Error while updating solr paint_db annotations!");
			e.printStackTrace();
		}
	}

	// Delete panther trees from local which don't have any plant genes in it. Save
	// all the deleted ids into a csv
	public void deleteTreesWithoutPlantGenes() throws Exception {
		int si = 1;
		pantherLocal.initLogWriter(0);
		while (si < 20000) {
			System.out.println("index " + si);
			List<FamilyNode> familyListBatch = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < familyListBatch.size(); i++) {
				String id = familyListBatch.get(i).getFamily_id();
				PantherData origPantherData = pantherLocal.readPantherTreeById(id);
				if (origPantherData != null) {
					// Has plant genome
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
			si = si + 1000;
		}
		pantherLocal.closeLogWriter(0);
	}

	// Process Pruned Tree Json String by updating values using local mapping files
	public String processPrunedTree(String jsonString) throws Exception {
		PantherData pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		// Mapping to convert tair locus ids to tair gene names
		HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
		Annotation rootNodeAnnotation = pantherData.getSearch().getAnnotation_node();
		rootNodeAnnotation = updatePantherTree(rootNodeAnnotation, tair_locus2id_mapping);
		pantherData.getSearch().setAnnotation_node(rootNodeAnnotation);
		// Convert Java Object to Json String
		ObjectMapper mapper = new ObjectMapper();
		String newJsonStr = mapper.writeValueAsString(pantherData);
		return newJsonStr;
	}

	// Update each node in the tree recursively.
	public Annotation updatePantherTree(Annotation node, HashMap<String, String> mapping) throws Exception {
		// Update Gene_id from mapping
		if (node.getChildren() != null) {
			for (int i = 0; i < node.getChildren().getAnnotation_node().size(); i++) {
				Annotation childNode = node.getChildren().getAnnotation_node().get(i);
				if (childNode.getGene_id() != null) {
					// System.out.println(childNode.getGene_id());
					String code = childNode.getGene_id().split(":")[0];
					if (code.equals("TAIR")) {
						String val = childNode.getGene_id().split(":")[1];
						if (val.split("=").length > 1) {
							val = val.split("=")[1];
							String updatedGeneId = mapping.get(val);
							childNode.setGene_id(code + ":" + updatedGeneId);
							// System.out.println(childNode.getGene_id());
						}
					}
				}
				updatePantherTree(childNode, mapping);
			}
		}
		return node;
	}

	private void updateLocusGeneNameById(String pantherId) throws Exception {
		HashMap<String, String> mapping = pantherLocal.read_locus2tair_mapping_csv();
		PantherData origPantherData = pantherLocal.readPantherTreeById(pantherId);
		if (origPantherData != null) {
			String familyName = "TEST";
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(
					origPantherData,
					familyName);
			List<String> gene_ids = modiPantherData.getGene_ids();
			System.out.println("gene_ids " + gene_ids.size());
			for (int j = 0; j < gene_ids.size(); j++) {
				String code = gene_ids.get(j).split(":")[0];
				if (code.equals("TAIR")) {
					String val = gene_ids.get(j).split(":")[1];
					val = val.split("=")[1];
					String updatedGeneId = mapping.get(val);
					System.out.println(val + " _ " + updatedGeneId);
					gene_ids.set(j, code + ":" + updatedGeneId);
				}
				modiPantherData.setGene_ids(gene_ids);
				pgServer.atomicUpdateSolr(pantherId, "gene_ids", modiPantherData.getGene_ids());
			}
			Annotation rootNodeAnnotation = modiPantherData.getSearch().getAnnotation_node();
			rootNodeAnnotation = updatePantherTree(rootNodeAnnotation, mapping);
			modiPantherData.getSearch().setAnnotation_node(rootNodeAnnotation);
			ObjectMapper mapper = new ObjectMapper();
			String newJsonStr = mapper.writeValueAsString(modiPantherData);
			String filename = pantherId + ".json";
			pgServer.uploadJsonToPGTreeBucket(filename, newJsonStr);
			System.out.println("Saved S3: " + pantherId);
			pantherLocal.saveSolrIndexedTreeAsFile(pantherId, newJsonStr);
		} else {
			System.out.println("No panther file found for " + pantherId);
		}
	}

	private void analyzePantherFamilies() throws Exception {
		List<String> pantherTree_ids = pantherLocal.getAllLocalPrunedTreeIds();
		System.out.println("pantherTree_ids Size " + pantherTree_ids.size());
		// List<String> pantherFamilyList_ids = pantherLocal.getAllLocalFamilyListIds();
		// System.out.println("pantherFamilyList_ids Size " +
		// pantherFamilyList_ids.size());
	}

	// Update gene names for tair ids (locus=2043813 => AT3G45780).
	public void updateLocusGeneNames() throws Exception {
		HashMap<String, String> mapping = pantherLocal.read_locus2tair_mapping_csv();
		int si = 1;
		while (si < 16001) {
			List<FamilyNode> pantherFamilyList = pantherLocal.getLocalPantherFamilyList(si);
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				String id = pantherFamilyList.get(i).getFamily_id();
				// if (id == "PTHR20835") {
				// System.out.println("Found family name");
				// }
				PantherData origPantherData = pantherLocal.readPantherTreeById(id);
				if (origPantherData != null) {
					String familyName = pantherFamilyList.get(i).getFamily_name();
					// System.out.println(familyName);
					PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(
							origPantherData,
							familyName);
					List<String> gene_ids = modiPantherData.getGene_ids();
					boolean needsUpdate = false;
					for (int j = 0; j < gene_ids.size(); j++) {
						String code = gene_ids.get(j).split(":")[0];
						if (code.equals("TAIR")) {
							String val = gene_ids.get(j).split(":")[1];
							val = val.split("=")[1];
							String updatedGeneId = mapping.get(val);
							gene_ids.set(j, code + ":" + updatedGeneId);
							needsUpdate = true;
						}
					}
					if (needsUpdate) {
						System.out.println("needsUpdate " + needsUpdate + " ID: " + id);
						modiPantherData.setGene_ids(gene_ids);
						pgServer.atomicUpdateSolr(id, "gene_ids", modiPantherData.getGene_ids());
						// Update s3 tree
						Annotation rootNodeAnnotation = modiPantherData.getSearch().getAnnotation_node();
						rootNodeAnnotation = updatePantherTree(rootNodeAnnotation, mapping);
						modiPantherData.getSearch().setAnnotation_node(rootNodeAnnotation);
						ObjectMapper mapper = new ObjectMapper();
						String newJsonStr = mapper.writeValueAsString(modiPantherData);
						String filename = id + ".json";
						pgServer.uploadJsonToPGTreeBucket(filename, newJsonStr);
						pantherLocal.saveSolrIndexedTreeAsFile(id, newJsonStr);
					}
				}
				if (i % 100 == 0) {
					System.out.println("processed " + i);
				}
			}
			si = si + 1000;
		}
	}

	public void generatePhyloXML() {
		pantherToPhyloXmlPipeline pxml = new pantherToPhyloXmlPipeline();
		pxml.convertAllInDirectory();
		pxml.uploadAlltoS3();
	}

	public void generateCsvs() throws Exception {
		pgServer.generateGenodoCsvAll();
		// pgServer.uploadAllPantherCSVtoS3();
	}

	// Generate csv files which analyzes panther etl dumps
	public void generate_analyze_dump() throws Exception {
		String filename = "panther17_dump_apr182022.csv";
		// pgServer.analyzePantherDump(filename);
		filename = "panther17_annos_apr182022.csv";
		pgServer.analyzePantherAnnotations2(filename);
	}

	public void indexSingleIdOnSolr(String id) throws Exception {
		// int si = 1;
		PantherData origPantherData = pantherLocal.readPantherTreeById(id);
		List<PantherData> pantherList = new ArrayList<>();
		if (origPantherData != null) {
			String familyName = origPantherData.getFamily_name();
			PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData,
					familyName);
			if (modiPantherData != null) {
				pantherList.add(modiPantherData);
			} else {
				System.out.println("Empty panther tree found " + id);
			}
			pgServer.saveAndCommitToSolr(pantherList);
			pantherList.clear();
			String jsonStr = modiPantherData.getJsonString();
			pantherLocal.saveSolrIndexedTreeAsFile(id, jsonStr);
			String filename = id + ".json";
			pgServer.uploadJsonToPGTreeBucket(filename, jsonStr);
		} else {
			System.out.println("File not found (Deleted)" + id);
		}
	}

	// Index solr db with panther trees saved locally, also save the json to S3
	// bucket
	public void indexSolrDB(boolean saveToS3) throws Exception {
		int si = 1;
		pantherLocal.initLogWriter(1);
		//////////////////// Logging //////////////////////////////////
		System.out.println("~~~~~~~~~~~~~~ Update Solr DB ~~~~~~~~~~~~~~~~");
		System.out.println("SOLR URL: " + pgServer.URL_SOLR);
		System.out.println("LOCAL PANTHER DATA FOLDER: " + pantherLocal.PATH_FAMILY_LIST);
		System.out.println("LOCAL PROCESSED TREE FOLDER: " + pantherLocal.PATH_LOCAL_SOLRTREE_JSON);
		if (saveToS3) {
			System.out.println("S3 BUCKET NAME: " + pgServer.PG_TREE_BUCKET_NAME);
		}
		System.out.println("~~~~~~~~~~~~~~ Update Solr DB ~~~~~~~~~~~~~~~~");
		pgServer.clearSolr();

		System.out.println("START IDX " + si);
		while (si < 16001) {
			List<FamilyNode> pantherFamilyList = pantherLocal.getLocalPantherFamilyList(si);
			List<PantherData> pantherList = new ArrayList<>();
			for (int i = 0; i < pantherFamilyList.size(); i++) {
				String id = pantherFamilyList.get(i).getFamily_id();
				PantherData origPantherData = pantherLocal.readPantherTreeById(id);
				if (origPantherData != null) {
					String familyName = pantherFamilyList.get(i).getFamily_name();
					PantherData modiPantherData = new PantherBookXmlToJson().convertJsonToSolrDocument(origPantherData,
							familyName);

					// Some panther trees might be empty after pruning, so we should not add it
					// solr.
					// We log the empty panther tree ids
					if (modiPantherData != null) {
						pantherList.add(modiPantherData);
					} else {
						System.out.println("Empty panther tree found " + id);
						pantherLocal.logEmptyId(id);
						continue;
					}
					System.out.println(modiPantherData.getId() + " idx: " + i + " size: "
							+ modiPantherData.getJsonString().length());
					pgServer.saveAndCommitToSolr(pantherList);
					pantherList.clear();

					String jsonStr = modiPantherData.getJsonString();
					pantherLocal.saveSolrIndexedTreeAsFile(id, jsonStr);
					// Save json string as local file
					if (saveToS3) {
						String filename = id + ".json";
						pgServer.uploadJsonToPGTreeBucket(filename, jsonStr);
					}
				} else {
					System.out.println("File not found (Deleted)" + id);
					// pantherLocal.logEmptyId(id);
					continue;
				}
			}
			si = si + 1000;
		}
		pantherLocal.closeLogWriter(1);
	}

	// save paralogs to 2 s3 buckets for json data and user download txt file
	// total time: < 15h
	public void saveParalogS3_tairids() throws IOException {
		HashMap<String, String> mapping = pantherLocal.load_tairid2uniprots_csv();
		Map<String, String> uniprot2tairid = mapping.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
		Map<String, String> mapping_revised = new HashMap<>();
		for (Map.Entry<String, String> entry : mapping.entrySet()) {
			String agi_id = entry.getKey();
			if (agi_id.contains("/")) {
				String[] agi_id_list = agi_id.split("/");
				for (String agi_id_item : agi_id_list) {
					mapping_revised.put(agi_id_item, entry.getValue());
				}
			} else {
				mapping_revised.put(agi_id, entry.getValue());
			}
		}
		HashMap<String, String> agi2symbol = pantherLocal.load_agi2symbol_json();
		System.out.println(mapping_revised.size());
		FileWriter nullCountFile = new FileWriter("panther_resources/nullCount_" + System.currentTimeMillis() + ".csv");
		CSVWriter paralogNullWriter = new CSVWriter(nullCountFile);
		String[] header = { "AGI ID", "AGI Nulls", "Primary Symbol Nulls", "Total Number",
				"Uniprot Ids with AGI Nulls" };
		paralogNullWriter.writeNext(header);
		paralogNullWriter.flush();
		int i = 0;
		for (Map.Entry<String, String> entry : mapping_revised.entrySet()) {
			// System.out.println(entry.getKey());
			if (entry.getKey().isEmpty())
				continue;
			i++;
			// if (i < 14667) continue;
			// if(!entry.getKey().equals("AT1G01010")) continue;

			// String[] sampleAGIAray = new String[]{"AT1G01010", "AT1G53790",
			// "AT3G26570","AT1G01130","AT1G01270"};
			// List<String> sampleAGIs = new ArrayList<>(Arrays.asList(sampleAGIAray));
			// if (!sampleAGIs.contains(entry.getKey())) continue;
			// String[] errorAGIAray = new String[]{"AT5G39690", "AT5G39540", "AT3G56560",
			// "AT3G56530", "AT3G55210", "AT1G32337"};
			// List<String> errorAGIs = new ArrayList<>(Arrays.asList(errorAGIAray));
			// if (!errorAGIs.contains(entry.getKey())) continue;

			// System.out.println(i);
			// if (i++ > 0) break;
			String uniprot_id = entry.getValue();
			try {
				List<String> paraResList = pantherServer.callParalog_uniprot(uniprot_id, uniprot2tairid, agi2symbol);
				// System.out.println("paraResList " + paraResList.toString());
				String paralog_json = paraResList.get(0);
				// System.out.println("paralog_json " + paralog_json);
				if (paralog_json == null) {
					System.out.println("Not saved " + entry.getKey());
				} else {
					String jsonFileName = entry.getKey() + ".json";
					pgServer.uploadJsonToPGParalogsBucket(jsonFileName, paralog_json);
					System.out.println("Saved " + jsonFileName + "-> " + Integer.toString(i));
				}
				String paralog_txt = paraResList.get(1);
				// System.out.println("paralog_txt " + paralog_txt);
				if (paralog_txt == null) {
					System.out.println("Not saved " + entry.getKey());
				} else {
					String txtFileName = entry.getKey() + "_paralog.txt";
					pgServer.uploadTxtToPGParalogsBucket(txtFileName, paralog_txt);
					System.out.println("Saved " + txtFileName + "-> " + Integer.toString(i));
				}

				String agiNullCount = paraResList.get(2);
				String primarySymbolNullCount = paraResList.get(3);
				String totalCount = paraResList.get(4);
				String agiNullUniprotIdsStr = paraResList.get(5);
				String agiId = entry.getKey();

				String[] nullCountLine = { agiId, agiNullCount, primarySymbolNullCount, totalCount,
						agiNullUniprotIdsStr };
				paralogNullWriter.writeNext(nullCountLine);
				paralogNullWriter.flush();
			} catch (Exception e) {
				System.out.println("Not saved " + entry.getKey());
				// System.out.println(e);
			}
		}
		paralogNullWriter.close();
	}

	// save orthologs to 2 s3 buckets for json data and user download txt file
	// total time: < 24h
	public void saveOrthologS3_tairids() {
		HashMap<String, String> mapping = pantherLocal.load_tairid2uniprots_csv();
		Map<String, String> uniprot2tairid = mapping.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
		Map<String, List<String>> organisms_mapping = pantherLocal.load_organisms_csv();
		Map<String, String> mapping_revised = new HashMap<>();
		for (Map.Entry<String, String> entry : mapping.entrySet()) {
			String agi_id = entry.getKey();
			if (agi_id.contains("/")) {
				String[] agi_id_list = agi_id.split("/");
				for (String agi_id_item : agi_id_list) {
					mapping_revised.put(agi_id_item, entry.getValue());
				}
			} else {
				mapping_revised.put(agi_id, entry.getValue());
			}
		}
		int i = 0;
		System.out.println("mapping_revised size: " + mapping_revised.size());
		for (Map.Entry<String, String> entry : mapping_revised.entrySet()) {
			if (entry.getKey().isEmpty())
				continue;
			i++;
			// // if(i < 22551) continue;
			String uniprot_id = entry.getValue();
			System.out.println("uniprot_id: " + uniprot_id);
			try {
				List<String> orthoResList = pantherServer.callOrtholog_uniprot(uniprot_id, uniprot2tairid,
						organisms_mapping);
				// System.out.println(orthoResList.toString());
				String ortho_json = orthoResList.get(0);
				String jsonFileName = entry.getKey() + ".json";
				pgServer.uploadJsonToPGOrthologsBucket(jsonFileName, ortho_json);
				System.out.println("Saved " + jsonFileName + " -> " + Integer.toString(i));
				String ortho_txt = orthoResList.get(1);
				String txtFileName = entry.getKey() + "_ortholog.txt";
				pgServer.uploadTxtToPGOrthologsBucket(txtFileName, ortho_txt);
				System.out.println("Saved " + txtFileName + " -> " + Integer.toString(i));
			} catch (Exception e) {
				System.out.println("Not saved " + entry.getKey());
				System.out.println(e);
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) throws Exception {
		long startTime = System.nanoTime();

		PantherETLPipeline etl = new PantherETLPipeline();

		// etl.storePantherFilesLocally();
		// etl.uploadToServer();

		// etl.updatePublicationsCount();

		// TASK: PHG-337: https://jira.phoenixbioinformatics.org/browse/PHG-327
		// etl.updateLocusGeneNames();
		// etl.updateLocusGeneNameById("PTHR20835");
		// etl.analyzePantherFamilies();

		// TASK: PHG-330: https://jira.phoenixbioinformatics.org/browse/PHG-330
		// etl.generatePhyloXML();

		// TASK: PHHG-331: https://jira.phoenixbioinformatics.org/browse/PHG-308
		etl.generateCsvs();

		// TASK: PHG-326: https://jira.phoenixbioinformatics.org/browse/PHG-326
		// etl.generate_analyze_dump();

		// etl.saveParalogS3_tairids();
		// etl.saveOrthologS3_tairids();

		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " +
				timeElapsed / 1000000);

		// etl.updateSolr_selected();
	}
}
