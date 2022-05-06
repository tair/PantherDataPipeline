package org.tair.process.panther;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVWriter;
import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import org.json.JSONArray;
import org.json.JSONObject;
import org.tair.module.*;
import org.tair.module.panther.Annotation;
import org.tair.module.panther.MSASequenceInfo;
import org.tair.module.panther.MsaData;
import org.tair.process.publications.PublicationsServerWrapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PhylogenesServerWrapper {
	private String RESOURCES_DIR = "src/main/resources";
	private String RESOURCES_BASE = "panther_resources";
	// S3 Keys
	String S3_ACCESS_KEY = "";
	String S3_SECRET_KEY = "";
	String PG_TREE_BUCKET_NAME = "";
	String PG_MSA_BUCKET_NAME = "";
	String PG_CSV_BUCKET_NAME = "";
	String PG_PARALOG_BUCKET_NAME = "phg-paralogs";
	String PG_ORTHO_BUCKET_NAME = "phg-orthologs";

	// private String URL_SOLR = "http://localhost:8983/solr/panther";
	// private String URL_SOLR = "http://52.37.99.223:8983/solr/panther";
	String URL_SOLR = "http://54.68.67.235:8983/solr/panther";

	SolrClient mysolr = null;
	AmazonS3 s3_server = null;
	int committedCount = 0;

	PantherLocalWrapper pantherLocal = new PantherLocalWrapper();
	PantherServerWrapper pantherServer = new PantherServerWrapper();

	public PhylogenesServerWrapper() {
		loadProps();
		mysolr = new HttpSolrClient.Builder(URL_SOLR).build();
		committedCount = 0;

		AWSCredentials credentials = new BasicAWSCredentials(S3_ACCESS_KEY, S3_SECRET_KEY);
		s3_server = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.US_WEST_2)
				.build();
	}

	private void loadProps() {
		try {
			InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
			// load props
			Properties prop = new Properties();
			prop.load(input);
			// System.out.println(prop);

			// if(prop.containsKey("URL_SOLR")) {
			// PG_MSA_BUCKET_NAME = prop.getProperty("URL_SOLR");
			// }
			if (prop.containsKey("RESOURCES_BASE")) {
				RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
				// makeDir(RESOURCES_BASE);
			}
			if (prop.containsKey("S3_ACCESS_KEY")) {
				S3_ACCESS_KEY = prop.getProperty("S3_ACCESS_KEY");
				System.out.println(S3_ACCESS_KEY);
			} else if (!System.getProperty("S3_ACCESS_KEY").isEmpty()) {
				S3_ACCESS_KEY = System.getProperty("S3_ACCESS_KEY");
			} else {
				System.out.println("S3_ACCESS_KEY not set!");
			}
			if (prop.containsKey("S3_SECRET_KEY")) {
				S3_SECRET_KEY = prop.getProperty("S3_SECRET_KEY");
			} else if (!System.getProperty("S3_SECRET_KEY").isEmpty()) {
				S3_SECRET_KEY = System.getProperty("S3_SECRET_KEY");
			} else {
				System.out.println("S3_SECRET_KEY not set!");
			}
			if (prop.containsKey("PG_TREE_BUCKET_NAME")) {
				PG_TREE_BUCKET_NAME = prop.getProperty("PG_TREE_BUCKET_NAME");
			}
			if (prop.containsKey("PG_MSA_BUCKET_NAME")) {
				PG_MSA_BUCKET_NAME = prop.getProperty("PG_MSA_BUCKET_NAME");
			}
			if (prop.containsKey("PG_CSV_BUCKET_NAME")) {
				PG_CSV_BUCKET_NAME = prop.getProperty("PG_CSV_BUCKET_NAME");
			}
		} catch (Exception e) {
			System.out.println("Prop file not found!");
			System.out.println("S3_ACCESS_KEY " + System.getenv("S3_ACCESS_KEY"));
			System.out.println("HOME " + System.getenv("HOME"));

			// if(!System.getenv("S3_SECRET_KEY").isEmpty()) {
			// S3_SECRET_KEY = System.getenv("S3_SECRET_KEY");
			// }
			// if(!System.getenv("S3_ACCESS_KEY").isEmpty()) {
			// S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY");
			// }
		}
	}

	public void makeDir(String dirPath) {
		File dir = new File(dirPath);
		if (!dir.isDirectory()) {
			dir.mkdir();
			System.out.println("Making dir " + dirPath);
		}
	}

	public void clearSolr() throws Exception {
		System.out.println("Clearing SOLR data .... ");
		mysolr.deleteByQuery("*:*");
		mysolr.commit();
		SolrQuery sq = new SolrQuery("*:*");
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
	}

	public void saveAndCommitToSolr(List<PantherData> pantherList) throws Exception {
		mysolr.addBeans(pantherList);
		mysolr.commit();
		committedCount += pantherList.size();
		System.out.println("Total file commited to solr until now " + committedCount);
	}

	public void uploadAllPantherCSVtoS3() {
		if (PG_CSV_BUCKET_NAME.isEmpty()) {
			System.out.println("PG_CSV_BUCKET_NAME not set in application.properties");
			return;
		}
		File dir = new File(RESOURCES_BASE + "/panther_csv");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			int fileCount = 0;
			for (File child : directoryListing) {
				if (child.getName().charAt(0) != '.') {// to ignore files such as .gitignore and .ds_store
					fileCount++;
					uploadCSVToPGCsvBucket(child.getName(), child);
					System.out.println("Saved S3: " + fileCount + " " + child.getName());
				}
			}
		}
	}

	// Update a specific field values for all trees saved in solr
	public void updateAllSolrTrees() throws Exception {
		HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "gene_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println(treeId);
			Object[] gene_ids = treeIdResponse.getResults().get(i).getFieldValues("gene_ids").toArray();
			List<String> new_gene_ids = new ArrayList<>();
			for (int j = 0; j < gene_ids.length; j++) {
				String gene_id = gene_ids[j].toString();
				String code = gene_id.split(":")[0];
				if (code.equals("TAIR")) {
					String locus_val = gene_id.split(":")[1];
					String local_val_id = locus_val.split("=")[1];
					String updatedGeneId = tair_locus2id_mapping.get(local_val_id);
					if (updatedGeneId != null) {
						System.out.println(gene_id + "->" + code + ":" + updatedGeneId);
						gene_id = code + ":" + updatedGeneId;
					} else {
						System.out.println(gene_id + " -> No Mapping found");
					}
				}
				new_gene_ids.add(gene_id);
			}
			atomicUpdateSolr(treeId, "gene_ids", new_gene_ids);
		}
	}

	// 04/215/2022 - A0A1U8BBT0
	public void updateAllSolrTreePubCounts() throws Exception {
		PublicationsServerWrapper psw = new PublicationsServerWrapper();
		SolrQuery sq = new SolrQuery("*:*");
		// SolrQuery sq = new SolrQuery("id:PTHR10263");
		sq.setRows(9000);
		sq.setFields("id", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("updating treeId " + treeId + " ...");
			Object[] uniprot_ids = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").toArray();

			List<String> publicationCountList = new ArrayList<String>();
			for (int j = 0; j < uniprot_ids.length; j++) {
				String uniprot_id = uniprot_ids[j].toString();
				// System.out.println("uni " + uniprot_id);
				Uniprot2PubMapping uni2pub = new Uniprot2PubMapping();
				List<String> pubs = psw.getPublicationsByUniprotId(uniprot_id);
				uni2pub.setPub_count(pubs.size());
				uni2pub.setUniprot_id(uniprot_id.toLowerCase());
				ObjectWriter ow = new ObjectMapper().writer();
				String goAnnotationDataStr = ow.writeValueAsString(uni2pub);
				publicationCountList.add(goAnnotationDataStr);
			}
			atomicUpdateSolr(treeId, "publications_count", publicationCountList);
		}
	}

	// Update a single field in solr
	public void atomicUpdateSolr(String id, String field_name, List<String> value) throws Exception {
		// Example code to update just one field
		SolrInputDocument sdoc = new SolrInputDocument();
		sdoc.addField("id", id);
		Map<String, List<String>> partialUpdate = new HashMap<>();
		partialUpdate.put("set", value);
		sdoc.addField(field_name, partialUpdate);
		mysolr.add(sdoc);
		mysolr.commit();
	}

	// Set "uniprot_ids_count" field for panther solr
	public void setUniprotIdsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(8900);
		sq.setFields("id, uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = mysolr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " " + treeId); // debugging visualization
			if (treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("uniprot_ids_count", partialUpdate);
				mysolr.add(sdoc);
				mysolr.commit();
			} else {
				System.out.println("No uniprot ids found");
			}
		}
	}

	// Set "go_annotations_count" field for panther solr
	public void setGoAnnotationsCount() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id, go_annotations");
		sq.setSort("id", SolrQuery.ORDER.asc);

		QueryResponse treeIdResponse = mysolr.query(sq);
		System.out.println(treeIdResponse.getResults().size());

		int totalDocsFound = treeIdResponse.getResults().size();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("processing: " + i + " " + treeId); // debugging visualization
			if (treeIdResponse.getResults().get(i).getFieldValues("go_annotations") != null) {
				int uniprotIdsCount = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").size();
				System.out.println(uniprotIdsCount);
				SolrInputDocument sdoc = new SolrInputDocument();
				Map<String, String> partialUpdate = new HashMap<>();
				partialUpdate.put("set", Integer.toString(uniprotIdsCount));
				sdoc.addField("id", treeId);
				sdoc.addField("go_annotations_count", partialUpdate);
				mysolr.add(sdoc);
				mysolr.commit();
			} else {
				System.out.println("null");
			}
		}
	}

	// XXX genes from XXX families have at least one GO annotation with the evidence
	// code IBA
	public void analyzePantherAnnos() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "go_annotations", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			if (treeIdResponse.getResults().get(i).getFieldValues("go_annotations") == null) {
				continue;
			}
		}
	}

	public void analyzePhyloXml15() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id");
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		String sourceDirectory = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes-storage/phyloXml_15";
		String deleteDirectory = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes-storage/phyloXml_15/deleted";
		File dir = new File(deleteDirectory);
		if (!dir.isDirectory()) {
			dir.mkdir();
			System.out.println("Making dir " + deleteDirectory);
		}
		File folder = new File(sourceDirectory);
		File[] listOfFiles = folder.listFiles();
		List<String> solr_ids = new ArrayList<>();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			solr_ids.add(treeId);
		}
		int in = 0;
		for (File file : listOfFiles) {
			if (file.isFile()) {
				// System.out.println(file.getName());
				String n = file.getName().replace(".xml", "");
				if (solr_ids.contains(n)) {
					// System.out.println(file.getName());
					in++;
				} else {
					String del_file = deleteDirectory + "/" + file.getName();
					if (file.renameTo(new File(del_file))) {
						// if file copied successfully then delete the original file
						file.delete();
						System.out.println("File moved successfully");
					} else {
						System.out.println("Failed to move the file " + n);
					}
				}
			}
		}
		System.out.println("total " + in);
		// for (int i = 0; i < totalDocsFound; i++) {
		// String treeId =
		// treeIdResponse.getResults().get(i).getFieldValue("id").toString();
		//
		//// Files.copy(sourceDirectory, targetDirectory);
		// }
	}

	public void analyzePGCsv() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id");
		QueryResponse treeIdResponse = mysolr.query(sq);
		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		String sourceDirectory = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes_data/PantherPipelineResources/panther16/phylogenes_csv";
		String deleteDirectory = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes_data/PantherPipelineResources/panther16/phylogenes_csv/deleted";
		File dir = new File(deleteDirectory);
		if (!dir.isDirectory()) {
			dir.mkdir();
			System.out.println("Making dir " + deleteDirectory);
		}
		List<String> solr_ids = new ArrayList<>();
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			solr_ids.add(treeId);
		}
		File folder = new File(sourceDirectory);
		File[] listOfFiles = folder.listFiles();
		for (File file : listOfFiles) {
			if (file.isFile()) {
				String n = file.getName().replace(".csv", "");
				if (!solr_ids.contains(n)) {
					String del_file = deleteDirectory + "/" + file.getName();
					if (file.renameTo(new File(del_file))) {
						// if file copied successfully then delete the original file
						file.delete();
						System.out.println("File moved successfully");
					} else {
						System.out.println("Failed to move the file " + n);
					}
				}
			}
		}
		List<String> file_ids = new ArrayList<>();
		for (File file : listOfFiles) {
			if (file.isFile()) {
				String n = file.getName().replace(".csv", "");
				file_ids.add(n);
			}
		}
		for (String s : solr_ids) {
			if (!file_ids.contains(s)) {
				System.out.println(s);
			}
		}
	}

	// Lists the panther trees, and number of total organisms per tree and list of
	// go annotations
	public void analyzePantherDump(String filename) throws Exception {
		// SolrQuery sq = new SolrQuery("id:PTHR23140");
		String filePath = RESOURCES_BASE + "/" + filename;
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "family_name", "sf_names", "go_annotations", "taxonomic_ranges");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);
		File file = new File(filePath);
		CsvWriter csvWriter = new CsvWriter();
		try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {
			List<String> cols = Arrays.asList("family ID", "family name", "subfamily name", "taxon range");
			for (int i = 0; i < cols.size(); i++) {
				csvAppender.appendField(cols.get(i));
			}
			List<String> organisms = pantherLocal.getOrganism_names();
			for (int i = 0; i < organisms.size(); i++) {
				csvAppender.appendField(organisms.get(i));
			}
			csvAppender.appendField("GO terms (F)");
			csvAppender.appendField("GO terms (P)");
			csvAppender.endLine();
			int totalDocsFound = treeIdResponse.getResults().size();
			System.out.println("totalDocsFound " + totalDocsFound);
			for (int i = 0; i < totalDocsFound; i++) {
				String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
				System.out.println(treeId);
				Object[] family_name = treeIdResponse.getResults().get(i).getFieldValues("family_name").toArray();
				Object[] sf_names = treeIdResponse.getResults().get(i).getFieldValues("sf_names").toArray();
				csvAppender.appendField(treeId);
				csvAppender.appendField(family_name[0].toString());
				String subsf_names = "";
				for (int j = 0; j < sf_names.length; j++) {
					subsf_names += sf_names[j].toString() + "|";
				}
				csvAppender.appendField(subsf_names);
				// taxon range
				if (treeIdResponse.getResults().get(i).getFieldValues("taxonomic_ranges") != null) {
					Object[] taxon_ranges = treeIdResponse.getResults().get(i).getFieldValues("taxonomic_ranges")
							.toArray();
					csvAppender.appendField(taxon_ranges[0].toString());
				} else {
					csvAppender.appendField("");
				}
				// Organisms
				Annotation root = pantherLocal.getPantherTreeRootById(treeId);
				if (root == null) {
					System.out.println("File not found " + treeId);
					for (int j = 0; j < organisms.size(); j++) {
						csvAppender.appendField("0");
					}
					csvAppender.appendField("");
					csvAppender.appendField("");
					csvAppender.endLine();
					continue;
				}
				// System.out.println(root);
				HashMap<String, Integer> organism_count = pantherLocal.getAllOrganismsFromTree(root);
				for (int j = 0; j < organisms.size(); j++) {
					if (organism_count.get(organisms.get(j)) != null) {
						int count = organism_count.get(organisms.get(j));
						// System.out.println(organisms.get(j) + " - " + count);
						csvAppender.appendField(Integer.toString(count));
					} else {
						csvAppender.appendField("0");
					}
				}
				// Go annotations
				if (treeIdResponse.getResults().get(i).getFieldValues("go_annotations") == null) {
					System.out.println("No Annotations");
					csvAppender.appendField("");
					csvAppender.appendField("");
					csvAppender.endLine();
				} else {
					Set<String> unique_anno_names_F = new LinkedHashSet<>();
					Set<String> unique_anno_names_P = new LinkedHashSet<>();
					Object[] go_annotations = treeIdResponse.getResults().get(i).getFieldValues("go_annotations")
							.toArray();
					if (go_annotations != null) {
						for (int j = 0; j < go_annotations.length; j++) {
							String annoStr = go_annotations[j].toString();
							JSONObject obj = new JSONObject(annoStr);
							String arrStr = obj.getString("go_annotations");

							JSONArray jsonArray = new JSONArray(arrStr);
							for (int k = 0; k < jsonArray.length(); k++) {
								try {
									GOAnno_new anno = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
											.readValue(jsonArray.getJSONObject(k).toString(), GOAnno_new.class);
									if (anno.getGoAspect().equals("F")
											|| anno.getGoAspect().equals("molecular_function")) {
										unique_anno_names_F.add(anno.getGoName());
									} else if (anno.getGoAspect().equals("P")
											|| anno.getGoAspect().equals("biological_process")) {
										unique_anno_names_P.add(anno.getGoName());
									}
								} catch (Exception e) {
									if (e.getMessage() != null) {
										System.out.println(e.getMessage());
									}
								}
							}
						}
						try {
							List<String> names_sorted = unique_anno_names_F.stream().collect(Collectors.toList());
							Collections.sort(names_sorted);
							String anno_row = String.join("|", names_sorted);
							System.out.println(anno_row);
							csvAppender.appendField(anno_row);
							names_sorted = unique_anno_names_P.stream().collect(Collectors.toList());
							Collections.sort(names_sorted);
							anno_row = String.join("|", names_sorted);
							System.out.println(anno_row);
							csvAppender.appendField(anno_row);
						} catch (Exception e) {
							System.out.println("names_sorted");
						}
					}
					csvAppender.endLine();
				}
			}
		}
	}

	// Lists pre tree: count of uniprots, genes and annotations
	public void analyzePantherAnnotations2(String filename) throws Exception {
		// SolrQuery sq = new SolrQuery("id:PTHR10012");
		String filePath = RESOURCES_BASE + "/" + filename;
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "go_annotations", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);
		File file = new File(filePath);
		CsvWriter csvWriter = new CsvWriter();
		try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {
			csvAppender.appendLine("treeId", "UniprotIds_count", "genes_count_1F", "genes_count_1P", "genes_count_1IBA",
					"goTerms_count_F", "goTerms_count_P");
			int totalDocsFound = treeIdResponse.getResults().size();
			System.out.println("totalDocsFound " + totalDocsFound);
			for (int i = 0; i < totalDocsFound; i++) {
				String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
				Object[] uniprot_ids = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").toArray();
				System.out.println(treeId + ":" + uniprot_ids.length);
				csvAppender.appendField(treeId);
				csvAppender.appendField(String.valueOf(uniprot_ids.length));

				if (treeIdResponse.getResults().get(i).getFieldValues("go_annotations") == null) {
					System.out.println("No Annotations");
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.endLine();
					continue;
				}

				Object[] go_annotations = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").toArray();
				int n_genes_with_onef = 0;
				int n_genes_with_oneb = 0;
				int n_genes_with_iba = 0;
				List<String> go_terms_f = new ArrayList<>();
				List<String> go_terms_b = new ArrayList<>();
				if (go_annotations != null) {
					for (int j = 0; j < go_annotations.length; j++) {
						String annoStr = go_annotations[j].toString();
						JSONObject obj = new JSONObject(annoStr);
						String arrStr = obj.getString("go_annotations");
						JSONArray jsonArray = new JSONArray(arrStr);
						boolean found_anno_f = false;
						boolean found_anno_p = false;
						boolean found_anno_iba = false;

						for (int k = 0; k < jsonArray.length(); k++) {
							try {
								GOAnno_new anno = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
										.readValue(jsonArray.getJSONObject(k).toString(), GOAnno_new.class);

								if (anno.getGoAspect().equals("F") || anno.getGoAspect().equals("molecular_function")) {
									if (!anno.getEvidenceCode().contains("IBA")) {
										if (!go_terms_f.contains(anno.getGoName())) {
											go_terms_f.add(anno.getGoName());
											// System.out.println("term f "+ anno.getGoName());
										}
										if (!found_anno_f) {
											found_anno_f = true;
											n_genes_with_onef++;
										}
									} else {
										if (!found_anno_iba) {
											found_anno_iba = true;
											n_genes_with_iba++;
										}
									}
								}
								if (anno.getGoAspect().equals("P") || anno.getGoAspect().equals("biological_process")) {
									if (!anno.getEvidenceCode().contains("IBA")) {
										if (!go_terms_b.contains(anno.getGoName())) {
											go_terms_b.add(anno.getGoName());
											// System.out.println("term b "+ anno.getGoName());
										}
										if (!found_anno_p) {
											found_anno_p = true;
											n_genes_with_oneb++;
										}
									} else {
										if (!found_anno_iba) {
											found_anno_iba = true;
											n_genes_with_iba++;
										}
									}
								}
							} catch (Exception e) {
								if (e.getMessage() != null) {
									System.out.println(e.getMessage());
								}
							}
						}
					}
					csvAppender.appendField(String.valueOf(n_genes_with_onef));
					csvAppender.appendField(String.valueOf(n_genes_with_oneb));
					csvAppender.appendField(String.valueOf(n_genes_with_iba));
					csvAppender.appendField(String.valueOf(go_terms_f.size()));
					csvAppender.appendField(String.valueOf(go_terms_b.size()));
					csvAppender.endLine();
					System.out.println(go_terms_f.size() + "," + go_terms_b.size());
					System.out.println(n_genes_with_onef + "," + n_genes_with_oneb);
					System.out.println("n_genes_with_iba " + n_genes_with_iba);
				} else {
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.endLine();
				}
			}
		}
	}

	public void analyzePantherAnnotations() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "go_annotations", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		File file = new File("annos_stats_aug19.csv");
		CsvWriter csvWriter = new CsvWriter();

		try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {
			csvAppender.appendLine("treeId", "UniprotIds_count", "molecular_count", "molecular_count_total",
					"biological_count", "biological_count_total");
			int totalDocsFound = treeIdResponse.getResults().size();
			System.out.println("totalDocsFound " + totalDocsFound);
			for (int i = 0; i < totalDocsFound; i++) {
				String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
				Object[] uniprot_ids = treeIdResponse.getResults().get(i).getFieldValues("uniprot_ids").toArray();
				System.out.println(treeId + ":" + uniprot_ids.length);
				csvAppender.appendField(treeId);
				csvAppender.appendField(String.valueOf(uniprot_ids.length));
				int molecular_count = 0;
				int biological_count = 0;
				int molecular_count_total = 0;
				int biological_count_total = 0;
				if (treeIdResponse.getResults().get(i).getFieldValues("go_annotations") == null) {
					System.out.println("No Annotations");
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.endLine();
					continue;
				}

				Object[] go_annotations = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").toArray();
				if (go_annotations != null) {
					for (int j = 0; j < go_annotations.length; j++) {
						String annoStr = go_annotations[j].toString();
						JSONObject obj = new JSONObject(annoStr);
						String arrStr = obj.getString("go_annotations");
						// System.out.println(arrStr);

						JSONArray jsonArray = new JSONArray(arrStr);

						for (int k = 0; k < jsonArray.length(); k++) {
							try {
								GOAnno_new anno = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
										.readValue(jsonArray.getJSONObject(k).toString(), GOAnno_new.class);
								// System.out.println(anno.getGoAspect().equals("F"));
								if (anno.getGoAspect().equals("F") || anno.getGoAspect().equals("molecular_function")) {
									// System.out.println(anno);
									if (!anno.getEvidenceCode().contains("IBA")) {
										molecular_count++;
									}
									molecular_count_total++;
								}
								if (anno.getGoAspect().equals("P") || anno.getGoAspect().equals("biological_process")) {
									if (!anno.getEvidenceCode().contains("IBA")) {
										biological_count++;
									}
									biological_count_total++;
								}

							} catch (Exception e) {
								if (e.getMessage() != null) {
									// System.out.println(e);
									System.out.println(e.getMessage());
								}
							}

						}
					}
					System.out.println(
							"molecular_count_total " + molecular_count_total + ", molecular_count " + molecular_count);
					csvAppender.appendField(String.valueOf(molecular_count));
					csvAppender.appendField(String.valueOf(molecular_count_total));
					System.out.println("biological_count_total " + biological_count_total + ", biological_count "
							+ biological_count);
					csvAppender.appendField(String.valueOf(biological_count));
					csvAppender.appendField(String.valueOf(biological_count_total));
					csvAppender.endLine();
				} else {
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.appendField(String.valueOf(0));
					csvAppender.endLine();
				}
			}
		}

	}

	// Analyze panther trees and find out all trees with Hori_Transfer node in it.
	// Write the ids to a csv
	public void analyzePantherTrees() throws Exception {
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id", "go_annotations");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);

		CSVWriter writer = pantherLocal.createLogWriter("tair_locus_ids.csv", "Tair Locus IDs");
		int totalLocusId = 0;
		int totalTrees = 0;
		for (int i = 0; i < totalDocsFound; i++) {
			Object[] src = treeIdResponse.getResults().get(i).getFieldValues("gene_ids").toArray();
			String[] gene_ids = new String[src.length];
			// System.out.println(Arrays.toString(gene_ids));
			System.arraycopy(src, 0, gene_ids, 0, src.length);
			for (int j = 0; j < gene_ids.length; j++) {
				if (gene_ids[j].contains("TAIR:")) {
					String genePrefix = gene_ids[j].split("=")[0];
					if (genePrefix.contains("locus")) {
						totalLocusId += 1;
					}
					String[] data = { gene_ids[j] };
					writer.writeNext(data);
				}
			}
			totalTrees += 1;
		}
		String totalLocusStr = "Total IDs containing 'locus=xxx': " + totalLocusId;
		String[] data = { totalLocusStr };
		writer.writeNext(data);
		String totalTreesStr = "Total Trees having this genes " + totalTrees;
		String[] data2 = { totalTreesStr };
		writer.writeNext(data2);
		writer.close();
		System.out.println("Total locus ids found " + totalLocusId);
		// String treeId =
		// treeIdResponse.getResults().get(i).getFieldValue("id").toString();

		// //Is Horizontal Transfer
		// boolean isHorizTransfer = new
		// PantherBookXmlToJson().isHoriz_Transfer(origPantherData);
		// if(isHorizTransfer) {
		// pantherLocal.logHTId(treeId);
		// }
		// }
		// pantherLocal.initLogWriter(2);
	}

	// TAIR-3504
	public void save_tairid2uniprot_mapping() {
		try {
			CSVWriter writer = pantherLocal.createLogWriter("tairid2uniprots_2.csv", "Tair_agi");
			writer.writeNext(new String[] { "tair_agi", "uniprots" });
			SolrQuery sq = new SolrQuery("*:*");
			sq.setRows(9500);
			sq.setFields("id");
			sq.setSort("id", SolrQuery.ORDER.asc);
			QueryResponse treeIdResponse = mysolr.query(sq);
			int totalDocsFound = treeIdResponse.getResults().size();
			HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
			HashMap<String, List<String>> tairid2uniprot_mapping = new HashMap<>();
			for (int i = 0; i < totalDocsFound; i++) {
				String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
				System.out.println(treeId);
				List<Annotation> leafNodes = pantherLocal.getAllLeafNodes(treeId);
				for (int j = 0; j < leafNodes.size(); j++) {
					Annotation currLeafNode = leafNodes.get(j);
					if (currLeafNode.getGene_id() != null) {
						// System.out.println(currLeafNode.getGene_id());
						if (currLeafNode.getOrganism().equals("Arabidopsis thaliana")) {
							String val = currLeafNode.getGene_id().split(":")[1];
							String updatedTairId = val;
							if (val.split("=").length > 1) {
								val = val.split("=")[1];
								updatedTairId = tair_locus2id_mapping.get(val);
							}

							String uniprotId = currLeafNode.get_uniprotId();
							if (tairid2uniprot_mapping.get(updatedTairId) == null) {
								List<String> matching_uniprots = new ArrayList<>();
								matching_uniprots.add(uniprotId);
								tairid2uniprot_mapping.put(updatedTairId, matching_uniprots);
							} else {
								System.out.println(tairid2uniprot_mapping.get(updatedTairId));
							}
						}
					}
				}
			}
			for (Map.Entry<String, List<String>> entry : tairid2uniprot_mapping.entrySet()) {
				String uniprots = String.join(",", entry.getValue());
				writer.writeNext(new String[] { entry.getKey(), uniprots });
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// all gene ids for arabidopsis gene
	public void save_geneid_arabidopsis() {
		try {
			CSVWriter writer = pantherLocal.createLogWriter("geneid_arabidopsis.csv", "Tair_geneId");
			writer.writeNext(new String[] { "gene_id" });
			SolrQuery sq = new SolrQuery("*:*");
			sq.setRows(9500);
			sq.setFields("id");
			sq.setSort("id", SolrQuery.ORDER.asc);
			QueryResponse treeIdResponse = mysolr.query(sq);
			int totalDocsFound = treeIdResponse.getResults().size();
			HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
			HashMap<String, List<String>> tairid2uniprot_mapping = new HashMap<>();
			for (int i = 0; i < totalDocsFound; i++) {
				String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
				System.out.println(treeId);
				List<Annotation> leafNodes = pantherLocal.getAllLeafNodes(treeId);
				for (int j = 0; j < leafNodes.size(); j++) {
					Annotation currLeafNode = leafNodes.get(j);
					// System.out.println(currLeafNode.getOrganism());
					if (currLeafNode.getOrganism().equals("Arabidopsis thaliana")) {
						// System.out.println(currLeafNode.getGene_id());
						writer.writeNext(new String[] { currLeafNode.getGene_id() });
					}
				}
			}
			// for (Map.Entry<String, List<String>> entry :
			// tairid2uniprot_mapping.entrySet()) {
			// String uniprots = String.join(",", entry.getValue());
			// writer.writeNext(new String[]{entry.getKey(), uniprots});
			// }
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void uploadJsonToPGTreeBucket(String filename, String jsonStr) {
		try {
			// System.out.println("PG_TREE_BUCKET_NAME " + PG_TREE_BUCKET_NAME);
			uploadJsonToS3(PG_TREE_BUCKET_NAME, filename, jsonStr);
		} catch (Exception e) {
			System.out.println("Failed to save to S3 " + e);
		}
	}

	public void uploadJsonToPGMsaBucket(String filename, String jsonStr) {
		try {
			uploadJsonToS3(PG_MSA_BUCKET_NAME, filename, jsonStr);
		} catch (Exception e) {
			System.out.println("Failed to save to S3 " + e);
		}
	}

	public void uploadCSVToPGCsvBucket(String filename, File file) {
		try {
			uploadObjectToBucket(PG_CSV_BUCKET_NAME, filename, file);
		} catch (Exception e) {
			System.out.println("Failed to save to S3 " + e);
		}
	}

	public void uploadJsonToPGParalogsBucket(String filename, String jsonStr) {
		try {
			uploadJsonToS3(PG_PARALOG_BUCKET_NAME, filename, jsonStr);
		} catch (Exception e) {
			System.out.println("Failed to save to S3 " + e);
		}
	}

	public void uploadJsonToPGOrthologsBucket(String filename, String jsonStr) {
		try {
			uploadJsonToS3(PG_ORTHO_BUCKET_NAME, filename, jsonStr);
		} catch (Exception e) {
			System.out.println("Failed to save to S3 " + e);
		}
	}

	/////////////////////////////////////////////////// S3 Calls
	public void createBucket(String bucket_name) throws Exception {
		if (s3_server.doesBucketExistV2(bucket_name)) {
			System.out.println(bucket_name + " already exists");
		}
		s3_server.createBucket(bucket_name);
	}

	public void listAllBuckets() {
		List<Bucket> buckets = s3_server.listBuckets();
		for (Bucket b : buckets) {
			System.out.println(b.getName());
		}
	}

	public void uploadObjectToBucket(String bucketName, String filename, File file) {
		s3_server.putObject(bucketName, filename, file);
	}

	public void uploadObjectToBucket(String bucketName, String filename, String filepath) {
		s3_server.putObject(bucketName, filename, new File(filepath));
	}

	public void uploadJsonToS3(String bucketName, String key, String content) {
		s3_server.putObject(bucketName, key, content);
	}

	private static String getAsString(InputStream is) throws IOException {
		if (is == null)
			return "";
		StringBuilder sb = new StringBuilder();
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(is, StringUtils.UTF8));
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} finally {
			is.close();
		}
		return sb.toString();
	}

	private List<String> getUniqueAnnoColNamesFromSolr(String familyId) throws Exception {
		SolrQuery sq = new SolrQuery("id:" + familyId);
		sq.setRows(9000);
		sq.setFields("id", "go_annotations", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);

		List<String> col_names = new ArrayList<>();
		if (totalDocsFound == 0) {
			System.out.println("No solr docs found for " + familyId);
			return col_names;
		}
		// System.out.println(treeIdResponse.getResults().get(0).getFieldValues("go_annotations")
		// == null);
		if (treeIdResponse.getResults().get(0).getFieldValues("go_annotations") == null) {
			return col_names;
		}
		Object[] go_annotations = treeIdResponse.getResults().get(0).getFieldValues("go_annotations").toArray();
		if (go_annotations != null) {
			// System.out.println("go length " + go_annotations.length);
			for (int j = 0; j < go_annotations.length; j++) {
				String annoStr = go_annotations[j].toString();
				JSONObject obj = new JSONObject(annoStr);

				String arrStr = obj.getString("go_annotations");
				// System.out.println(arrStr);
				JSONArray jsonArray = new JSONArray(arrStr);
				for (int k = 0; k < jsonArray.length(); k++) {
					GOAnno_new anno = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
							.readValue(jsonArray.getJSONObject(k).toString(), GOAnno_new.class);
					if (anno.getGoAspect() != null) {
						String col_name = anno.getGoName() + " (" + anno.getGoId() + ")";
						if (!col_names.contains(col_name)) {
							col_names.add(col_name);
						}
					}
				}
			}
			System.out.println("col names " + col_names.size());
		}
		java.util.Collections.sort(col_names, String.CASE_INSENSITIVE_ORDER);
		return col_names;
	}

	private HashMap<String, List<GOAnno_new>> getAnnotationsMappingFromSolr(String familyId) throws Exception {
		// annotations
		SolrQuery sq = new SolrQuery("id:" + familyId);
		sq.setRows(9000);
		sq.setFields("id", "go_annotations", "uniprot_ids");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		// System.out.println("totalDocsFound " + totalDocsFound);

		HashMap<String, List<GOAnno_new>> uni_mapping = new HashMap<>();

		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			if (treeIdResponse.getResults().get(0).getFieldValues("go_annotations") == null) {
				return uni_mapping;
			}
			Object[] go_annotations = treeIdResponse.getResults().get(i).getFieldValues("go_annotations").toArray();
			if (go_annotations != null) {
				System.out.println("go length " + go_annotations.length);
				for (int j = 0; j < go_annotations.length; j++) {
					String annoStr = go_annotations[j].toString();
					JSONObject obj = new JSONObject(annoStr);

					String arrStr = obj.getString("go_annotations");
					// System.out.println(arrStr);
					JSONArray jsonArray = new JSONArray(arrStr);

					for (int k = 0; k < jsonArray.length(); k++) {
						GOAnno_new anno = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
								.readValue(jsonArray.getJSONObject(k).toString(), GOAnno_new.class);
						// System.out.println(anno.getGeneProductId() + " - " + anno.getGoName());
						String uniprotId = anno.getGeneProductId().toUpperCase();
						if (!uni_mapping.containsKey(uniprotId)) {
							List<GOAnno_new> annos = new ArrayList<>();
							annos.add(anno);
							uni_mapping.put(uniprotId, annos);
						} else {
							List<GOAnno_new> annos = uni_mapping.get(uniprotId);
							annos.add(anno);
							uni_mapping.put(uniprotId, annos);
						}
					}
				}
			}

			// System.out.println("mapping processed");
		}

		return uni_mapping;
	}

	public void generateGenodoCsvAll() throws Exception {
		String filename = RESOURCES_BASE + "/panther_csv/";
		makeDir(filename);
		SolrQuery sq = new SolrQuery("*:*");
		sq.setRows(9000);
		sq.setFields("id");
		sq.setSort("id", SolrQuery.ORDER.asc);
		QueryResponse treeIdResponse = mysolr.query(sq);

		int totalDocsFound = treeIdResponse.getResults().size();
		System.out.println("totalDocsFound " + totalDocsFound);
		for (int i = 0; i < totalDocsFound; i++) {
			String treeId = treeIdResponse.getResults().get(i).getFieldValue("id").toString();
			System.out.println("Generating " + treeId);
			parsePantherTreeFromS3(treeId);
		}
	}

	public void parsePantherTreeFromS3(String familyId) throws Exception {
		HashMap<String, String> tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
		// load column names
		List<String> anno_cols = getUniqueAnnoColNamesFromSolr(familyId);
		System.out.println("anno_col_names " + anno_cols.size());
		List<String> cols = new ArrayList<String>(
				Arrays.asList("Uniprot ID", "Gene", "Gene ID", "Gene name", "Organism", "Subfamily name"));
		cols.addAll(anno_cols);

		HashMap<String, List<GOAnno_new>> uni_mapping = getAnnotationsMappingFromSolr(familyId);

		Annotation curr_node = getPantherTreeRootById(familyId);
		if (curr_node == null) {
			System.out.println("stop " + familyId);
		}
		List<Annotation> leaf_nodes = new ArrayList<>();
		leaf_nodes = pantherLocal.iterate_getAllLeafNodes(curr_node, leaf_nodes);
		// System.out.println(PG_TREE_BUCKET_NAME + "tree " + root);
		// System.out.println("leaf_nodes size "+ leaf_nodes.size());

		// System.out.println("cols length " + cols_list.size());
		String filename = RESOURCES_BASE + "/panther_csv/" + familyId + ".csv";

		File file = new File(filename);
		CsvWriter csvWriter = new CsvWriter();
		String intro_txt = "Columns after 'Subfamily name', if any, are GO annotations. Each column is a GO molecular function or biological process term that is annotated to at least one member of the gene family AND the annotation is supported by an experimental evidence (indicated by 'EXP') or phylogenetic inference (indicated by 'IBA'). A '0' indicates absence of either annotations.";
		try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {
			csvAppender.appendField(intro_txt);
			csvAppender.endLine();
			// System.out.println(cols.size());
			for (int i = 0; i < cols.size(); i++) {
				csvAppender.appendField(cols.get(i));
			}
			csvAppender.endLine();

			for (int i = 0; i < leaf_nodes.size(); i++) {
				Annotation node = leaf_nodes.get(i);
				if (node.getGene_symbol() == null) {
					// System.out.println("getGene_id null");
				}

				String gene_id = node.getGene_id().split(":")[1];
				String type = node.getGene_id().split(":")[0];
				if (type.equals("TAIR")) {
					// System.out.println("gene_id "+ gene_id);
					String locusId = gene_id.split("=")[1];
					gene_id = tair_locus2id_mapping.get(locusId);
				}

				// System.out.println("done 1");
				String gene = gene_id;
				if (node.getGene_symbol() != null) {
					gene = node.getGene_symbol();
				}
				// System.out.println("uniprot "+ node.get_uniprotId());
				csvAppender.appendField(node.get_uniprotId());

				csvAppender.appendField(gene);
				csvAppender.appendField(gene_id);
				csvAppender.appendField(node.getGene_symbol());
				csvAppender.appendField(node.getOrganism());
				csvAppender.appendField(node.getSf_name());
				List<GOAnno_new> matchingAnnos = new ArrayList<>();
				if (uni_mapping.containsKey(node.get_uniprotId())) {
					matchingAnnos = uni_mapping.get(node.get_uniprotId());
				} else {
					// System.out.println("No mapping found " + node.get_uniprotId());
				}
				// System.out.println("matching annoes done");
				for (int j = 6; j < cols.size(); j++) {
					String anno_present = "0";
					String col_name = cols.get(j);
					col_name = col_name.split("\\(")[0];
					col_name = col_name.trim();
					for (int k = 0; k < matchingAnnos.size(); k++) {
						GOAnno_new anno = matchingAnnos.get(k);
						if (anno.getGoName().equals(col_name)) {
							if (anno.getEvidenceCode().contains("IBA")) {
								if (anno_present != "EXP") {
									anno_present = "IBA";
								}
							} else {
								anno_present = "EXP";
							}
						}
					}
					csvAppender.appendField(anno_present);
				}
				csvAppender.endLine();
			}
			// System.out.println(root.get_uniprotId() + root.getGene_id());
			//
		} catch (Exception e) {
			System.out.println("Excepion " + e.getMessage());
		}
	}

	public Annotation getPantherTreeRootById(String familyId) throws Exception {
		System.out.println("BUCKET NAME " + PG_TREE_BUCKET_NAME + " familyId:" + familyId);
		try {
			S3Object fullObject = s3_server.getObject(new GetObjectRequest(PG_TREE_BUCKET_NAME, familyId + ".json"));
			S3ObjectInputStream s3is = fullObject.getObjectContent();
			String str = getAsString(s3is);
			JSONObject jsonObject = new JSONObject(str);
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String k = keys.next();
				// System.out.println(k);
				if (k.equals("jsonString")) {
					// System.out.println(jsonObject.get(k));
					String jsonString = (String) jsonObject.get(k);
					PantherData pantherStructureData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
							.readValue(jsonString,
									PantherData.class);
					// System.out.println(pantherStructureData.getSearch().getAnnotation_node());
					return pantherStructureData.getSearch().getAnnotation_node();
				} else if (k.equals("search")) {
					PantherData pantherStructureData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
							.readValue(str,
									PantherData.class);
					return pantherStructureData.getSearch().getAnnotation_node();
				}
			}
			return null;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public String getFastaDocForPrunedTree(String treeId, int[] selected_taxons) throws Exception {
		String jsonString = pantherServer.readPrunedPantherTreeById(treeId, selected_taxons);
		PantherData pantherStructureData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
				.readValue(jsonString, PantherData.class);
		// System.out.println(pantherStructureData.getSearch().getAnnotation_node().getSf_name());
		Annotation root = pantherStructureData.getSearch().getAnnotation_node();
		return get_processed_msa(root, treeId);
	}

	public String getFastaDocFromTree(String treeId) throws Exception {
		Annotation root = getPantherTreeRootById(treeId);
		return get_processed_msa(root, treeId);
	}

	public String get_processed_msa(Annotation root, String treeId) throws Exception {
		HashMap<String, String> persistentId2fasta = pantherLocal.mapPersistentIds(root);
		String fileName = treeId + ".json";

		S3Object fullObject = s3_server.getObject(new GetObjectRequest(PG_MSA_BUCKET_NAME, fileName));
		// System.out.println("Content-Type: " +
		// fullObject.getObjectMetadata().getContentType());
		S3ObjectInputStream s3is = fullObject.getObjectContent();
		String str = getAsString(s3is);
		JSONObject jsonObject = new JSONObject(str);

		Iterator<String> keys = jsonObject.keys();
		String fasta_doc = "";
		while (keys.hasNext()) {
			String k = keys.next();
			if (jsonObject.get(k) instanceof JSONArray) {
				JSONArray familyNames = (JSONArray) jsonObject.get(k);
				ObjectMapper mapper = new ObjectMapper();
				String MsaString = (String) familyNames.getJSONObject(0).get("msa_data");
				MsaData msa = mapper.enable(SerializationFeature.INDENT_OUTPUT).readValue(MsaString, MsaData.class);
				// System.out.println(msa.getSequenceList().size());
				int count = 0;
				for (int i = 0; i < msa.getSequenceList().size(); i++) {
					MSASequenceInfo seq = msa.getSequenceList().get(i);
					if (persistentId2fasta.get(seq.getPersistent_id()) != null) {
						count = count + 1;
						fasta_doc += ">" + persistentId2fasta.get(seq.getPersistent_id()) + "\n";
						String sequence = seq.getSequence();
						sequence = sequence.replaceAll("(.{60})", "$1\n");
						fasta_doc += sequence;
						fasta_doc += "\n";
						// System.out.println(seq.getPersistent_id() + "-" +
						// persistentId2fasta.get(seq.getPersistent_id()));
					}
				}
			}
		}
		return fasta_doc;
	}

	public void downloadMSAJsonFromS3(String treeId) throws Exception {
		String fasta_doc = getFastaDocFromTree(treeId);
		PrintWriter out = new PrintWriter(treeId + ".txt");
		out.println(fasta_doc);
		out.close();
	}

	public static void main(String args[]) {
		PhylogenesServerWrapper pgServer = new PhylogenesServerWrapper();
		try {
			// pgServer.analyzePantherDump();
			// pgServer.updateAllSolrTrees();
			// pgServer.getFastaDocFromTree("PTHR22166");
			// int[] taxon_array = {13333,3702};
			// pgServer.getFastaDocForPrunedTree("PTHR22166", taxon_array);
			// pgServer.analyzePhyloXml15();
			// pgServer.analyzePGCsv();
			pgServer.save_tairid2uniprot_mapping();
			// pgServer.save_geneid_arabidopsis();

			// PTHR10037
			// pgServer.parsePantherTreeFromS3("PTHR10492");
			// pgServer.generateGenodoCsvAll();

			// pgServer.updateAllSolrTreePubCounts();

		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
