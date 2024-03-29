package org.tair.process.panther;

import com.amazonaws.services.dynamodbv2.xspec.S;
import com.amazonaws.services.snowball.model.Ec2RequestFailedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.CSVWriter;
import com.opencsv.CSVReader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;
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
    // Change resources base to your local resources panther folder
    private String RESOURCES_BASE = "panther_resources";

    // Change this to the location of where you have saved panther data
    String PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList/";
    private String PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
    private String PATH_LOCAL_MSA_DATA = RESOURCES_BASE + "/msa_jsons/";
    String PATH_LOCAL_SOLRTREE_JSON = RESOURCES_BASE + "/solr_trees_files/";

    private String PATH_HT_LIST = RESOURCES_BASE + "/familyHTList.csv";
    private String PATH_NP_LIST = RESOURCES_BASE + "/familyNoPlantsList.csv";
    private String PATH_EMPTY_LIST = RESOURCES_BASE + "/familyEmptyWhileIndexingList.csv";
    private String PATH_LOCUSID_TAIR_MAPPING = "/AGI_locusId_mapping_20200410.csv";
    private String NAME_TAIRID_UNIPROTS_MAPPING = "/tairid2uniprots.csv";
    private String ORGANISMS_MAPPING = "/organisms_for_homologs.csv";
    private String PATH_ORG_MAPPING = "/organism_to_display.csv";
    // log family that has large msa data
    private String PATH_LARGE_MSA_LIST = RESOURCES_BASE + "/largeMsaFamilyList.csv";
    // log family that has invalid msa data
    private String PATH_INVALID_MSA_LIST = RESOURCES_BASE + "/invalidMsaFamilyList.csv";
    private String PATH_NAME_AGI_SYMBOL_MAPPING = "/symbols.json";

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
        // System.out.println(PATH_NP_LIST);
        mapper = new ObjectMapper();
        csvFile_noplants = new File(PATH_NP_LIST);
        csvFile_empty = new File(PATH_EMPTY_LIST);
        csvFile_ht = new File(PATH_HT_LIST);
        this.process_locus2tairId_mapping();
        this.process_organism_mapping();
    }

    private void loadProps() {
        try {
            InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
            // load props
            Properties prop = new Properties();
            prop.load(input);
            // System.out.println(prop);
            if (prop.containsKey("RESOURCES_BASE")) {
                RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
                makeDir(RESOURCES_BASE);
            }
            initPaths();
            // System.out.println(PATH_LOCAL_PRUNED_TREES);
        } catch (Exception e) {
            // System.out.println("PantherLocalWrapper: Prop file not found!");
        }
    }

    private void initPaths() {
        PATH_FAMILY_LIST = RESOURCES_BASE + "/familyList/";
        PATH_LOCAL_PRUNED_TREES = RESOURCES_BASE + "/pruned_panther_files/";
        PATH_LOCAL_MSA_DATA = RESOURCES_BASE + "/msa_jsons/";
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
        System.out.println("PATH_LOCUSID_TAIR_MAPPING " + PATH_LOCUSID_TAIR_MAPPING);
        try {
            File csv_tair_mapping = new File(getClass().getResource(PATH_LOCUSID_TAIR_MAPPING).toURI());
            CSVReader reader = new CSVReader(new FileReader(csv_tair_mapping), ' ');
            // read line by line
            String[] record = null;
            while ((record = reader.readNext()) != null) {
                String agiId = record[0].split(",")[0];
                String locusId = record[0].split(",")[1];
                // System.out.println(agiId);
                locus2tairId_mapping.put(locusId, agiId);
            }
        } catch (Exception e) {
            System.out.println("process_locus2tairId_mapping: Error");
        }
    }

    private void process_organism_mapping() {
        organism_mapping = new HashMap<String, String>();
        organism_names = new ArrayList<>();
        String[] record = null;
        try {
            File csv_org_mapping = new File(getClass().getResource(PATH_ORG_MAPPING).toURI());
            CSVReader reader = new CSVReader(new FileReader(csv_org_mapping), ',');
            int i = 0;
            while ((record = reader.readNext()) != null) {
                String org = record[0];
                if (i != 0) {
                    organism_names.add(org);
                }
                i++;
                String org_code = record[3];
                if (!record[2].isEmpty()) {
                    org += " (" + record[2] + ")";
                }
                // System.out.println(org + "-" + org_code);
                organism_mapping.put(org_code, org);
            }
        } catch (Exception e) {
            System.out.println("process_organism_mapping: Error");
        }
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
        } catch (IOException e) {
            makeDir(PATH_FAMILY_LIST);
            Util.saveJsonStringAsFile(jsonString, filename);
        }
    }

    public String loadFamilyListBatch(int startIdx) throws Exception {
        String filename = PATH_FAMILY_LIST + "familyList_" + startIdx + ".json";
        return Util.loadJsonStringFromFile(filename);
    }

    // Get Panther Family List from local files if it exists. With new api the
    // family list is saved as batches of 1000 ids.
    public List<FamilyNode> getLocalPantherFamilyList(int start_index) throws Exception {
        String filename = "familyList_" + start_index + ".json";
        String filepath = PATH_FAMILY_LIST + filename;
        InputStream input = new FileInputStream(filepath);

        String data = mapper.readValue(input, String.class);

        PantherFamilyList flJson = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(data,
                PantherFamilyList.class);

        List<FamilyNode> allFamilies = flJson.getFamilyNodes();
        System.out.format("Found %d families from %s \n", allFamilies.size(), filename);
        return allFamilies;
    }

    public boolean doesPantherTreeExist(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
        File tempFile = new File(filePath);
        return tempFile.exists();
    }

    public boolean isPantherTreeDeleted(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + "Deleted/" + familyId + ".json";
        File tempFile = new File(filePath);
        return tempFile.exists();
    }

    public void savePantherDataAsJsonFile(PantherData pantherData, String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
        try {
            Util.saveJavaObjectAsFile(pantherData, filePath);
        } catch (IOException e) {
            makeDir(PATH_LOCAL_PRUNED_TREES);
            try {
                Util.saveJavaObjectAsFile(pantherData, filePath);
            } catch (Exception ie) {
                System.out.println("File saving failed! " + familyId);
            }
        } catch (Exception e) {
            System.out.println("File saving failed! " + familyId);
        }
    }

    public String getMSAJsonFile(String familyId) throws Exception {
        try {
            String fileName = familyId + ".json";
            String json_filepath = PATH_LOCAL_MSA_DATA + "/" + fileName;
            return Util.loadJsonStringFromFile(json_filepath);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public String saveMSADataAsJsonFile(String familyId, String msaData) throws Exception {
        System.out.println("familyId " + familyId + " msaData " + msaData.length());
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
        } catch (IOException e) {
            makeDir(PATH_LOCAL_MSA_DATA);
            Util.saveJsonStringAsFile(jo.toString(), json_filepath);
        }
        return jo.toString();
    }

    public void saveSolrIndexedTreeAsFile(String familyId, String solrTree) throws Exception {
        String fileName = familyId + ".json";
        String json_filepath = PATH_LOCAL_SOLRTREE_JSON + "/" + fileName;
        // System.out.println("json_filepath " + json_filepath);
        try {
            Util.saveJsonStringAsFile(solrTree, json_filepath);
        } catch (IOException e) {
            makeDir(PATH_LOCAL_SOLRTREE_JSON);
            Util.saveJsonStringAsFile(solrTree, json_filepath);
        }
    }

    public Annotation getPantherTreeRootById(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
        // System.out.println(filePath);
        InputStream input = null;
        try {
            input = new FileInputStream(filePath);
            PantherData data = mapper.readValue(input, PantherData.class);
            // Converts json string to a PantherData SearchResult object.
            PantherData pantherStructureData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(
                    data.getJsonString(),
                    PantherData.class);
            return pantherStructureData.getSearch().getAnnotation_node();
        } catch (Exception e) {
            System.out.println("File reading failed! " + e);
            return null;
        }
    }

    private HashMap<String, Integer> iterate_node(Annotation node, HashMap<String, Integer> organism_count) {
        for (int i = 0; i < node.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = node.getChildren().getAnnotation_node().get(i);
            if (child_node.getTree_node_type().equals("LEAF")) {
                int count = organism_count.containsKey(child_node.getOrganism())
                        ? organism_count.get(child_node.getOrganism())
                        : 0;
                organism_count.put(child_node.getOrganism(), count + 1);
            }
            if (child_node.getChildren() != null) {
                organism_count = iterate_node(child_node, organism_count);
            }
        }
        return organism_count;
    }

    private HashMap<String, String> iterate_node_mapPersistentIds(Annotation node,
            HashMap<String, String> persistentId2fasta) {
        for (int i = 0; i < node.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = node.getChildren().getAnnotation_node().get(i);
            if (child_node.getTree_node_type().equals("LEAF")) {
                String persistent_id = child_node.getPersistent_id();
                // System.out.println(child_node.get_uniprotId());
                String fasta_header = child_node.get_uniprotId() + "|" + child_node.getOrganism() + "|"
                        + child_node.get_extractedGeneId();
                persistentId2fasta.put(persistent_id, fasta_header);
            }
            if (child_node.getChildren() != null) {
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
        return organism_count;
    }

    public List<Annotation> iterate_getAllLeafNodes(Annotation currNode, List<Annotation> added_nodes) {
        if (currNode.getChildren() == null)
            return added_nodes;
        for (int i = 0; i < currNode.getChildren().getAnnotation_node().size(); i++) {
            Annotation child_node = currNode.getChildren().getAnnotation_node().get(i);
            if (child_node != null && child_node.getTree_node_type() != null) {
                if (child_node.getTree_node_type().equals("LEAF")) {
                    // System.out.println(child_node.get_extractedGeneId());
                    added_nodes.add(child_node);
                }
                if (child_node.getChildren() != null) {
                    added_nodes = iterate_getAllLeafNodes(child_node, added_nodes);
                }
            }
        }
        return added_nodes;
    }

    public List<Annotation> getAllLeafNodes(String familyId) {
        Annotation root = getPantherTreeRootById(familyId);
        if (root == null) {
            System.out.println("root is null");
            return new ArrayList<>();
        }
        List<Annotation> leaf_nodes = new ArrayList<>();
        leaf_nodes = iterate_getAllLeafNodes(root, leaf_nodes);
        // System.out.println("All leaf nodes: "+ leaf_nodes.size());
        return leaf_nodes;
    }

    public PantherData readPantherTreeById(String familyId) {
        String filePath = PATH_LOCAL_PRUNED_TREES + familyId + ".json";
        InputStream input = null;
        try {
            input = new FileInputStream(filePath);
            return mapper.readValue(input, PantherData.class);
        } catch (Exception e) {
            // System.out.println("File reading failed! " + e);
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
        if (logType == 0) {
            System.out.println("Init csvFile_noplants Logs " + csvFile_noplants);
            FileWriter outputfile = new FileWriter(csvFile_noplants);
            deleteNoPlantsWriter = new CSVWriter(outputfile);
            String[] header = { "Deleted No Plants Ids" };
            deleteNoPlantsWriter.writeNext(header);
        } else if (logType == 1) {
            System.out.println("Init Logs " + csvFile_empty);
            FileWriter outputfile = new FileWriter(csvFile_empty);
            emptyTreeCsvWriter = new CSVWriter(outputfile);
            String[] header = { "Ids with empty tree from server" };
            emptyTreeCsvWriter.writeNext(header);
        } else if (logType == 2) {
            FileWriter outputfile = new FileWriter(csvFile_ht);
            HTListCsvWriter = new CSVWriter(outputfile);
            String[] header = { "Ids with horizontal transfer" };
            HTListCsvWriter.writeNext(header);
        }
    }

    public CSVWriter createLogWriter(String filename, String headerStr) throws Exception {
        String newLogFile = RESOURCES_BASE + "/" + filename;
        File csvFile_custom = new File(newLogFile);
        System.out.println("Log file created " + newLogFile);
        FileWriter outputfile = new FileWriter(csvFile_custom);
        // String[] header = {headerStr};
        CSVWriter currWriter = new CSVWriter(outputfile);
        ;
        // currWriter.writeNext(header);
        return currWriter;
    }

    public CSVWriter createLogWriter(String filename, String[] headers) throws Exception {
        String newLogFile = RESOURCES_BASE + "/" + filename;
        File csvFile_custom = new File(newLogFile);
        System.out.println("Log file created " + newLogFile);
        FileWriter outputfile = new FileWriter(newLogFile);
        String[] header = headers;
        CSVWriter currWriter = new CSVWriter(outputfile);
        ;
        currWriter.writeNext(header);
        return currWriter;
    }

    public HashMap<String, String> read_locus2tair_mapping_csv() throws Exception {
        return this.locus2tairId_mapping;
    }

    public HashMap<String, String> read_org_mapping_csv() throws Exception {
        return this.organism_mapping;
    }

    public void logDeletedId(String id) throws Exception {
        String[] data = { id };
        deleteNoPlantsWriter.writeNext(data);
    }

    public void logEmptyId(String id) throws Exception {
        String[] data = { id };
        emptyTreeCsvWriter.writeNext(data);
    }

    public void logHTId(String id) throws Exception {
        String[] data = { id };
        HTListCsvWriter.writeNext(data);
    }

    public void closeLogWriter(int logType) throws Exception {
        if (logType == 0) {
            deleteNoPlantsWriter.close();
        } else if (logType == 1) {
            emptyTreeCsvWriter.close();
        } else if (logType == 2) {
            HTListCsvWriter.close();
        }
    }

    public void makeDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.isDirectory()) {
            dir.mkdir();
            System.out.println("Making dir " + dirPath);
        }
    }

    // "tairid2uniprots.csv" : eg. AT2G40450 -> O22890f
    public HashMap<String, String> load_tairid2uniprots_csv() {
        String csv_path = RESOURCES_BASE + NAME_TAIRID_UNIPROTS_MAPPING;
        System.out.println("load_tairid2uniprots_csv => " + csv_path);
        HashMap<String, String> tairid2uniprots_mapping = new HashMap<String, String>();
        String[] record = null;
        try {
            CSVReader reader = new CSVReader(new FileReader(csv_path), ',');
            // System.out.println(reader.readNext());
            while ((record = reader.readNext()) != null) {
                tairid2uniprots_mapping.put(record[0], record[1]);
            }
            return tairid2uniprots_mapping;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public List<String> getAllLocalPrunedTreeIds() {
        File folder = new File(PATH_LOCAL_PRUNED_TREES);
        File[] listOfFiles = folder.listFiles();
        List<String> panther_ids = new ArrayList<>();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                // System.out.println(file.getName());
                if (file.getName().contains(".json")) {
                    // System.out.println(file.getName().split(".json")[0]);
                    String id = file.getName().split(".json")[0];
                    // System.out.println(id);s
                    if (id.equals("PTHR45637")) {
                        System.out.println("Found " + id);
                    }
                    panther_ids.add(id);
                }
            }
        }
        return panther_ids;
    }

    // "organisms_for_homologs.csv" :
    // eg. JUGRE (organism) -> Juglans regia (full name), walnut
    // (common name),
    // eudicotyledons (group name)
    public HashMap<String, List<String>> load_organisms_csv() {
        String csv_path = RESOURCES_BASE + ORGANISMS_MAPPING;
        System.out.println("load_organisms_csv => " + csv_path);
        HashMap<String, List<String>> organisms_mapping = new HashMap<String, List<String>>();
        String[] record = null;
        try {
            CSVReader reader = new CSVReader(new FileReader(csv_path), ',');
            while ((record = reader.readNext()) != null) {
                // System.out.println(record[3]);
                organisms_mapping.put(record[3], Arrays.asList(record[0], record[2], record[6]));
            }
            // Logs
            // Iterator it = organisms_mapping.entrySet().iterator();
            // while (it.hasNext()) {
            // Map.Entry pair = (Map.Entry) it.next();
            // System.out.println(pair.getKey() + " = " + pair.getValue());
            // }

            return organisms_mapping;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public HashMap<String, String> load_agi2symbol_json() {
        String agi2symbol_path = RESOURCES_BASE + PATH_NAME_AGI_SYMBOL_MAPPING;
        System.out.println(agi2symbol_path);
        HashMap<String, String> agi2symbol_mapping = new HashMap<String, String>();
        String[] record = null;
        try {
            JSONTokener tokener = new JSONTokener(new FileReader(agi2symbol_path));
            JSONArray symbolJsonArray = new JSONArray(tokener);
            Iterator<Object> iterator = symbolJsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject symbolJsonObj = (JSONObject) iterator.next();
                agi2symbol_mapping.put(symbolJsonObj.get("locusName").toString(),
                        symbolJsonObj.get("symbolName").toString());
            }
            return agi2symbol_mapping;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public static void main(String args[]) throws Exception {
        PantherLocalWrapper lw = new PantherLocalWrapper();
        // Annotation root = lw.getPantherTreeRootById("PTHR10012");
        lw.getAllLeafNodes("PTHR10012");
        // lw.getAllOrganismsFromTree(root);
        // lw.initLogWriter(0);
        // lw.read_mapping_csv();
    }

}
