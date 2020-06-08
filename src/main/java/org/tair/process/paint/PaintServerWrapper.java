package org.tair.process.paint;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.tair.module.GOAnnotationData;
import org.tair.module.paint.PaintAnnotationData;
import org.tair.module.paint.UniqueAnnotation;
import org.tair.module.paint.flatfile.GoBasic;
import org.tair.module.paint.flatfile.Nodes;
import org.tair.module.paint.flatfile.PaintAnno;
import org.tair.util.Util;

import java.io.*;
import java.util.*;

public class PaintServerWrapper {
    String url = "http://paintcuration.usc.edu/services/tree/node/annotation/go/";
    ObjectMapper mapper;
    private String RESOURCES_DIR = "src/main/resources";
    private String GO_BASIC_PATH = RESOURCES_DIR + "/go-basic.json";
    //Change resources base to your local resources panther folder
    private String RESOURCES_BASE = "/Users/swapp1990/Documents/projects/Pheonix_Projects/phylogenes_data";
    private String goAnnoCsvName = "Pthr_GO_14.1.tsv";

    private String solrUrl = "http://localhost:8983/solr";
    private SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();

    PaintServerWrapper() {
        mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        makeDir(RESOURCES_DIR+"/"+RESOURCES_BASE);
    }

//    private void loadProps() {
//        try {
//            InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
//            // load props
//            Properties prop = new Properties();
//            prop.load(input);
//            if(prop.containsKey("RESOURCES_BASE")) {
//                RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
//                makeDir(RESOURCES_BASE);
//            }
//        } catch (Exception e) {
//            System.out.println("Prop file not found!");
//        }
//    }

    PaintAnnotationData getGoAnnotationFromUrl(String url) {
        try {
            String jsonString = Util.readContentFromWebJsonToJson(url);
            PaintAnnotationData annotationObj = mapper.readValue(jsonString, PaintAnnotationData.class);
            System.out.println(annotationObj.getUniqueAnnotations());
            return annotationObj;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    void getGoAnnotationListFromPersistentId(String persId) {
        try {
            String paintUrl = url+persId;
            System.out.println(persId);
            String jsonString = Util.readContentFromWebJsonToJson(paintUrl);
            PaintAnnotationData annotationObj = mapper.readValue(jsonString, PaintAnnotationData.class);
            System.out.println(annotationObj.getUniqueAnnotations().size());
            if(annotationObj.getUniqueAnnotations().size() > 0) {
                List<UniqueAnnotation> annos = annotationObj.getUniqueAnnotations();
//                annos.forEach();
            }
//            return annotationObj;
        } catch (Exception e) {
            System.out.println("Failed " + persId);
            e.printStackTrace();
//            return null;
        }
    }

    void storeGoAnnosLocally(String persId) {
        try {
            String paintUrl = url+persId;
            String jsonString = Util.readContentFromWebJsonToJson(paintUrl);
            PaintAnnotationData annotationObj = mapper.readValue(jsonString, PaintAnnotationData.class);
            if(annotationObj.getUniqueAnnotations().size() > 0) {
                String filename = RESOURCES_DIR + "/" + RESOURCES_BASE + "/" + persId + ".json";
                Util.saveJsonStringAsFile(jsonString, filename);
//                System.out.println("Saved " + persId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    PaintAnno processPaintAnno(String[] cols, GoBasic go_basic) {
        if(cols.length < 3) return null;
        String[] db_references = new String[]{"PMID", "GO_REF", "DOI", "AGRICOLA_IND"};
        System.out.println(Arrays.toString(cols));
//        System.out.println(cols[1]  + ";" + cols[2] + ";" + cols[4]);
        String qualifier = "";
        String evidence_code = cols[2];
        String reference_id = "";
        if(cols.length == 4) {
            reference_id = cols[3];
        } else if(cols.length == 5) {
            reference_id = cols[4];
        }
        String reference_db = reference_id.split(":")[0];
        boolean matchedRefDb = Arrays.asList(db_references).contains(reference_db);
        if(!matchedRefDb) {
            System.out.println("Not matched " + matchedRefDb);
            return null;
        }
//        System.out.println("qualifier " + qualifier + " evidence_code " + evidence_code);
        PaintAnno anno = new PaintAnno();
        //UniprotId
        String uniprotId = cols[0].split("UniProtKB=")[1];
        anno.setGeneProductId(uniprotId);
        //Evidence Code
        anno.setEvidenceCode(evidence_code);
        //GoId
        anno.setGoId(cols[1]);
        String go_id = cols[1].split("GO:")[1];
        //GoName
        boolean goLabelFound = false;
        for(int i=0; i<go_basic.getNodes().size(); i++) {
            Nodes nodes = go_basic.getNodes().get(i);
            if(nodes.getGoId() != null && nodes.getGoId().equals(go_id)) {
//                        System.out.println("Go Label: "+ nodes.getLbl());
                anno.setGoName(nodes.getLbl());
                goLabelFound = true;
//                        System.out.println("Aspects: "+ nodes.getGoAspects().toString());
                if(nodes.getGoAspects().contains("molecular_function")) {
                    anno.setGoAspect("molecular_function");
                } else if(nodes.getGoAspects().contains("biological_process")) {
                    anno.setGoAspect("biological_process");
                }
            }
        }
        if(!goLabelFound) {
            System.out.println("Go Label not foubd!");
        }
        anno.setReference(reference_id);
        return anno;
    }

    void readTsvFile() throws Exception {
        GoBasic go_basic = Util.loadJsonFromFile(GO_BASIC_PATH);
//        System.out.println(obj.getNodes().size());
//        for(int i=0; i<obj.getNodes().size(); i++) {
//            Nodes nodes = obj.getNodes().get(i);
//            System.out.println(nodes.getGoId() + " : " + nodes.getProcessVal());
//        }
        String csv_path = RESOURCES_BASE + "/" + goAnnoCsvName;
        String[] evidence_codes = new String[]{"EXP", "IDA", "IEP", "IGI", "IMP", "IPI"};
        // remove all data from this collection
        solrClient.deleteByQuery("paint_db", "*:*");
        try (BufferedReader br = new BufferedReader(new FileReader(csv_path))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split("\\s+");
                boolean matchedEvidence = Arrays.asList(evidence_codes).contains(cols[2]);
                if(matchedEvidence) {
                    PaintAnno anno = processPaintAnno(cols, go_basic);
                    if(anno == null) {
                        System.out.println("Anno null ");
                        continue;
                    }
                    count = count+1;
                    System.out.println(count);

                    SolrInputDocument doc = new SolrInputDocument();
                    doc.addField("uniprot_id", anno.getGeneProductId());
                    Map<String, List<String>> partialUpdate = new HashMap<>();
                    ObjectWriter ow = new ObjectMapper().writer();
                    String goAnnotationDataStr = ow.writeValueAsString(anno);
                    doc.addField("go_annotations", goAnnotationDataStr);
                    solrClient.add("paint_db", doc);
                    solrClient.commit("paint_db");
                } else {
//                    System.out.println("Not matched " + cols[2]);
                }
            }
            System.out.println(count);
        }
    }

    void testUrl() {
        String testId = "PTN002493370";
        url = url + testId;
        getGoAnnotationFromUrl(url);
    }

    public void makeDir(String dirPath) {
        File dir = new File(dirPath);
        if(!dir.isDirectory()) {
            dir.mkdir();
            System.out.println("Making dir "+ dirPath);
        }
    }

    public void testLocalFile() throws Exception{
        readTsvFile();
    }

    public static void main(String args[]) throws Exception {
        PaintServerWrapper server = new PaintServerWrapper();
        server.testLocalFile();
//        server.testUrl();
    }
}
