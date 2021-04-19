package org.tair.process.panther;

import com.amazonaws.services.dynamodbv2.xspec.S;
import com.amazonaws.services.snowball.model.Ec2RequestFailedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVWriter;
import com.opencsv.CSVReader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.tair.module.Children;
import org.tair.module.FamilyNode;
import org.tair.module.PantherData;
import org.tair.module.PantherFamilyList;
import org.tair.module.panther.Annotation;
import org.tair.module.pantherForPhylo.Panther;
import org.tair.util.Util;

import java.io.*;
import java.util.*;

public class PantherLocalWrapper {
    private String RESOURCES_DIR = "src/main/resources";
    private String WEB_RESOURCES_DIR = "WEB-INF";
    //Change resources base to your local resources panther folder
    private String RESOURCES_BASE = "panther_resources";

    //Change this to the location of where you have saved panther data
    private String PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList/";
    private String PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
    private String PATH_LOCAL_MSA_DATA = RESOURCES_BASE +"/msa_jsons/";
    private String PATH_LOCAL_SOLRTREE_JSON = RESOURCES_BASE + "/solr_trees_files/";

    private String PATH_HT_LIST = RESOURCES_BASE + "/familyHTList.csv";
    private String PATH_NP_LIST = RESOURCES_BASE + "/familyNoPlantsList.csv";
    private String PATH_EMPTY_LIST = RESOURCES_BASE + "/familyEmptyWhileIndexingList.csv";
    private String PATH_LOCUSID_TAIR_MAPPING = "/AGI_locusId_mapping_20200410.csv";
    private String NAME_TAIRID_UNIPROTS_MAPPING = "/tairid2uniprots.csv";
    private String PATH_ORG_MAPPING = "/organism_to_display.csv";
    // log family that has large msa data
    private String PATH_LARGE_MSA_LIST = RESOURCES_BASE + "/largeMsaFamilyList.csv";
    // log family that has invalid msa data
    private String PATH_INVALID_MSA_LIST = RESOURCES_BASE + "/invalidMsaFamilyList.csv";

    ObjectMapper mapper;
    File csvFile_noplants;
    File csvFile_empty;
    File csvFile_ht;
    CSVWriter deleteNoPlantsWriter;
    CSVWriter emptyTreeCsvWriter;
    CSVWriter HTListCsvWriter;

    private HashMap<String, String> locus2tairId_mapping;
    private HashMap<String, String> tairId2uniprotId_mapping;
    private HashMap<String, String> organism_mapping;
    private List<String> organism_names;

    public PantherLocalWrapper() {
        loadProps();
//        System.out.println(PATH_NP_LIST);
        mapper = new ObjectMapper();
        csvFile_noplants = new File(PATH_NP_LIST);
        csvFile_empty = new File(PATH_EMPTY_LIST);
        csvFile_ht = new File(PATH_HT_LIST);
        this.process_locus2tairId_mapping();
        this.process_organism_mapping();

//        System.out.println("Locus2idmapping loaded "+ locus2tairId_mapping.size());
//        System.out.println("Orgidmapping loaded "+ organism_mapping.size());
//
        System.out.println("RESOURCES_BASE " + RESOURCES_BASE);
    }

    private void loadProps() {
        try {
            InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
            // load props
            Properties prop = new Properties();
            prop.load(input);
//            System.out.println(prop);
            if(prop.containsKey("RESOURCES_BASE")) {
                RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
                makeDir(RESOURCES_BASE);
            }
            initPaths();
        } catch (Exception e) {
//            System.out.println("Prop file not found!");
        }
    }

    private void initPaths() {
        PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList/";
        PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
        PATH_LOCAL_MSA_DATA = RESOURCES_BASE +"/msa_jsons/";
        PATH_LOCAL_SOLRTREE_JSON = RESOURCES_BASE + "/solr_trees_files/";

        PATH_HT_LIST = RESOURCES_BASE + "/familyHTList.csv";
        PATH_NP_LIST = RESOURCES_BASE + "/familyNoPlantsList.csv";
        PATH_EMPTY_LIST = RESOURCES_BASE + "/familyEmptyWhileIndexingList.csv";
        // log family that has large msa data
        PATH_LARGE_MSA_LIST = RESOURCES_BASE + "/largeMsaFamilyList.csv";
        // log family that has invalid msa data
        PATH_INVALID_MSA_LIST = RESOURCES_BASE + "/invalidMsaFamilyList.csv";
    }

    private void process_locus2tairId_mapping() {
        locus2tairId_mapping = new HashMap<String, String>();
        try {
        File csv_tair_mapping = new File(getClass().getResource(PATH_LOCUSID_TAIR_MAPPING).toURI());
        CSVReader reader = new CSVReader(new FileReader(csv_tair_mapping), ' ');
        // read line by line
        String[] record = null;
        while ((record = reader.readNext()) != null) {
            String agiId = record[0].split(",")[0];
            String locusId = record[0].split(",")[1];
//                System.out.println(agiId);
            locus2tairId_mapping.put(locusId, agiId);
        }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void process_organism_mapping() {
        organism_mapping = new HashMap<String, String>();
        organism_names = new ArrayList<>();
        String[] record = null;
        try {
            File csv_org_mapping = new File(getClass().getResource(PATH_ORG_MAPPING).toURI());
            CSVReader reader = new CSVReader(new FileReader(csv_org_mapping), ',');
            int i=0;
            while ((record = reader.readNext()) != null) {
                String org = record[0];
                if(i != 0) {
                    organism_names.add(org);
                }
                i++;
                String org_code = record[3];
                if(!record[2].isEmpty()) {
                    org += " (" + record[2] + ")";
                }
//                System.out.println(org + "-" + org_code);
                organism_mapping.put(org_code, org);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void process_tairId2uniprot_mapping() {
        tairId2uniprotId_mapping = new HashMap<String, String>();

    }

    public String getLocalFamiliListPath() {
        return PATH_FAMILY_LIST;
    }
    public String getLocalPrunedTreesPath() {
        return PATH_LOCAL_PRUNED_TREES;
    }
    public String getLocalMSAPath() {
        return PATH_LOCAL_MSA_DATA;
    }
    public List<String> getOrganism_names() {
        return organism_names;
    }

    public void savePantherFamilyListBatch(String jsonString, int startIdx) throws Exception {
        String filename = PATH_FAMILY_LIST + "familyList_" + startIdx + ".json";
        try {
            Util.saveJsonStringAsFile(jsonString, filename);
        } catch(IOException e) {
            makeDir(PATH_FAMILY_LIST);
            Util.saveJsonStringAsFile(jsonString, filename);
        }
    }

    public String loadFamilyListBatch(int startIdx) throws Exception {
        String filename = PATH_FAMILY_LIST + "familyList_" + startIdx + ".json";
        return Util.loadJsonStringFromFile(filename);
    }

    //Get Panther Family List from local files if it exists. With new api the family list is saved as batches of 1000 ids.
	public List<FamilyNode> getLocalPantherFamilyList(int start_index) throws Exception {
        String filename = "familyList_"+start_index+".json";
	    String filepath = PATH_FAMILY_LIST + filename;
		InputStream input = new FileInputStream(filepath);

		String data = mapper.readValue(input, String.class);

		PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
				PantherFamilyList.class);

		List<FamilyNode> allFamilies = flJson.getFamilyNodes();
		System.out.format("Found %d families from %s \n", allFamilies.size(), filename);
		return allFamilies;
	}

	public void savePantherDataAsJsonFile(PantherData pantherData, String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId+ ".json";
        try {
            Util.saveJavaObjectAsFile(pantherData, filePath);
        }
        catch(IOException e) {
            makeDir(PATH_LOCAL_PRUNED_TREES);
            try {
                Util.saveJavaObjectAsFile(pantherData, filePath);
            } catch(Exception ie) {
                System.out.println("File saving failed! " + familyId);
            }
        }
        catch(Exception e) {
            System.out.println("File saving failed! " + familyId);
        }
    }

    public String saveMSADataAsJsonFile(String familyId, String msaData) throws Exception {
        System.out.println("familyId " + familyId + " msaData "+ msaData.length());
        String fileName = familyId + ".json";
        String json_filepath = PATH_LOCAL_MSA_DATA + "/" + fileName;

        JSONObject jo = new JSONObject();
        Collection<JSONObject> items = new ArrayList<JSONObject>();
        JSONObject item = new JSONObject();
        item.put("id", familyId);
        item.put("msa_data", msaData);
        items.add(item);
        jo.put("familyNames", new JSONArray(items));
        try {
            Util.saveJsonStringAsFile(jo.toString(), json_filepath);
        } catch(IOException e) {
            makeDir(PATH_LOCAL_MSA_DATA);
            Util.saveJsonStringAsFile(jo.toString(), json_filepath);
        }
        return jo.toString();
    }

    public void saveSolrIndexedTreeAsFile(String familyId, String solrTree) throws Exception {
        String fileName = familyId + ".json";
        String json_filepath = PATH_LOCAL_SOLRTREE_JSON + "/" + fileName;
        try {
            Util.saveJsonStringAsFile(solrTree, json_filepath);
        } catch(IOException e) {
            makeDir(PATH_LOCAL_SOLRTREE_JSON);
            Util.saveJsonStringAsFile(solrTree, json_filepath);
        }
    }

    public Annotation getPantherTreeRootById(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
        InputStream input = null;
        try {
            input = new FileInputStream(filePath);
            PantherData data = mapper.readValue(input, PantherData.class);
            //Converts json string to a PantherData SearchResult object.
            PantherData pantherStructureData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data.getJsonString(),
                    PantherData.class);
            return pantherStructureData.getSearch().getAnnotation_node();
        }
        catch(Exception e) {
            System.out.println("File reading failed! " + e);
            return null;
        }
    }

    private HashMap<String, Integer> iterate_node(Annotation node, HashMap<String, Integer> organism_count) {
        for(int i=0; i<node.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = node.getChildren().getAnnotation_node().get(i);
            if(child_node.getTree_node_type().equals("LEAF")) {
                int count = organism_count.containsKey(child_node.getOrganism()) ? organism_count.get(child_node.getOrganism()) : 0;
                organism_count.put(child_node.getOrganism(), count + 1);
            }
            if(child_node.getChildren() != null) {
                organism_count = iterate_node(child_node, organism_count);
            }
        }
        return organism_count;
    }

    private HashMap<String, String> iterate_node_mapPersistentIds(Annotation node, HashMap<String, String> persistentId2fasta) {
        for(int i=0; i<node.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = node.getChildren().getAnnotation_node().get(i);
            if(child_node.getTree_node_type().equals("LEAF")) {
                String persistent_id = child_node.getPersistent_id();
//                System.out.println(child_node.get_uniprotId());
                String fasta_header = child_node.get_uniprotId() + "|" + child_node.getOrganism() + "|" + child_node.get_extractedGeneId();
                persistentId2fasta.put(persistent_id, fasta_header);
            }
            if(child_node.getChildren() != null) {
                persistentId2fasta = iterate_node_mapPersistentIds(child_node, persistentId2fasta);
            }
        }
        return persistentId2fasta;
    }

    public HashMap<String, String> mapPersistentIds(Annotation root) {
        HashMap<String, String> persistentId2fasta = new HashMap<String, String>();
        try {
            persistentId2fasta = iterate_node_mapPersistentIds(root, persistentId2fasta);
        } catch (Exception e) {
            System.out.println("getAllOrganismsFromTree except");
        }
        return persistentId2fasta;
    }

    public HashMap<String, Integer> getAllOrganismsFromTree(Annotation root) {
        HashMap<String, Integer> organism_count = new HashMap<>();
        try {
            organism_count = iterate_node(root, organism_count);
        } catch (Exception e) {
            System.out.println("getAllOrganismsFromTree except");
        }
//        organism_count.forEach((key, value) -> System.out.println(key + ":" + value));
        return organism_count;
    }

    private List<Annotation> iterate_getAllLeafNodes(Annotation currNode, List<Annotation> added_nodes) {
        if(currNode.getChildren() == null) return added_nodes;
        for(int i=0; i<currNode.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = currNode.getChildren().getAnnotation_node().get(i);
            if(child_node != null && child_node.getTree_node_type() != null) {
                if(child_node.getTree_node_type().equals("LEAF")) {
//                System.out.println(child_node.get_extractedGeneId());
                    added_nodes.add(child_node);
                }
                if(child_node.getChildren() != null) {
                    added_nodes = iterate_getAllLeafNodes(child_node, added_nodes);
                }
            }

        }
        return added_nodes;
    }

    public List<Annotation> getAllLeafNodes(String familyId) {
        Annotation root = getPantherTreeRootById(familyId);
        List<Annotation> leaf_nodes = new ArrayList<>();
        leaf_nodes = iterate_getAllLeafNodes(root, leaf_nodes);
//        System.out.println("All leaf nodes: "+ leaf_nodes.size());
        return leaf_nodes;
    }

    public PantherData readPantherTreeById(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
		InputStream input = null;
		try {
            input = new FileInputStream(filePath);
            return mapper.readValue(input, PantherData.class);
        }
        catch(Exception e) {
//            System.out.println("File reading failed! " + e);
            return null;
        }
    }

    public void deleteFile(String id) {
        makeDir(PATH_LOCAL_PRUNED_TREES + "Deleted/");
        String filePath = PATH_LOCAL_PRUNED_TREES + id + ".json";
        String filePathToMove = PATH_LOCAL_PRUNED_TREES + "Deleted/" + id + ".json";
        File fileToDelete = new File(filePath);
        fileToDelete.renameTo(new File(filePathToMove));
    }

    public void initLogWriter(int logType) throws Exception {
        if(logType == 0) {
            FileWriter outputfile = new FileWriter(csvFile_noplants);
            deleteNoPlantsWriter = new CSVWriter(outputfile);
            String[] header = {"Deleted No Plants Ids"};
            deleteNoPlantsWriter.writeNext(header);
        } else if(logType == 1) {
            FileWriter outputfile = new FileWriter(csvFile_empty);
            emptyTreeCsvWriter = new CSVWriter(outputfile);
            String[] header = {"Ids with empty tree from server"};
            emptyTreeCsvWriter.writeNext(header);
        } else if(logType == 2) {
            FileWriter outputfile = new FileWriter(csvFile_ht);
            HTListCsvWriter = new CSVWriter(outputfile);
            String[] header = {"Ids with horizontal transfer"};
            HTListCsvWriter.writeNext(header);
        }
    }

    public CSVWriter createLogWriter(String filename, String headerStr) throws Exception {
        String newLogFile = RESOURCES_BASE + "/" + filename;
        File csvFile_custom = new File(newLogFile);
        System.out.println("Log file created "+ newLogFile);
        FileWriter outputfile = new FileWriter(csvFile_custom);
//        String[] header = {headerStr};
        CSVWriter currWriter = new CSVWriter(outputfile);;
//        currWriter.writeNext(header);
        return currWriter;
    }

    public CSVWriter createLogWriter(String filename, String[] headers) throws Exception {
        String newLogFile = RESOURCES_BASE + "/" + filename;
        File csvFile_custom = new File(newLogFile);
        System.out.println("Log file created "+ newLogFile);
        FileWriter outputfile = new FileWriter(newLogFile);
        String[] header = headers;
        CSVWriter currWriter = new CSVWriter(outputfile);;
        currWriter.writeNext(header);
        return currWriter;
    }

    public HashMap<String, String> read_locus2tair_mapping_csv() throws Exception {
        return this.locus2tairId_mapping;
    }

    public HashMap<String, String> read_org_mapping_csv() throws Exception {
        return this.organism_mapping;
    }

    public void logDeletedId(String id) throws Exception{
        String[] data = {id};
        deleteNoPlantsWriter.writeNext(data);
    }

    public void logEmptyId(String id) throws Exception{
        String[] data = {id};
        emptyTreeCsvWriter.writeNext(data);
    }

    public void logHTId(String id) throws Exception{
        String[] data = {id};
        HTListCsvWriter.writeNext(data);
    }

    public void closeLogWriter(int logType) throws Exception {
        if(logType == 0) {
            deleteNoPlantsWriter.close();
        } else if(logType == 1) {
            emptyTreeCsvWriter.close();
        } else if(logType == 2) {
            HTListCsvWriter.close();
        }
    }

    public void makeDir(String dirPath) {
        File dir = new File(dirPath);
        if(!dir.isDirectory()) {
            dir.mkdir();
            System.out.println("Making dir "+ dirPath);
        }
    }

    //"tairid2uniprots.csv" : eg. AT2G40450	-> O22890
    public HashMap<String, String> load_tairid2uniprots_csv() {
        String csv_path = RESOURCES_BASE + NAME_TAIRID_UNIPROTS_MAPPING;
//        System.out.println(csv_path);
        HashMap<String, String> tairid2uniprots_mapping = new HashMap<String, String>();
        String[] record = null;
        try {
            CSVReader reader = new CSVReader(new FileReader(csv_path), ',');
//            System.out.println(reader.readNext());
            while ((record = reader.readNext()) != null) {
                tairid2uniprots_mapping.put(record[0], record[1]);
            }
            return tairid2uniprots_mapping;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }

    }



    public static void main(String args[]) throws Exception {
        PantherLocalWrapper lw = new PantherLocalWrapper();
//        Annotation root = lw.getPantherTreeRootById("PTHR10012");
        lw.getAllLeafNodes("PTHR10012");
//        lw.getAllOrganismsFromTree(root);
//        lw.initLogWriter(0);
//        lw.read_mapping_csv();
    }


}
