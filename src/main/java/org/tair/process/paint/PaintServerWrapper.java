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

    PaintServerWrapper() {

    }

    PaintAnno processPaintAnno(String[] cols, GoBasic go_basic) {
        if(cols.length < 3) return null;
        String[] db_references = new String[]{"PMID", "GO_REF", "DOI", "AGRICOLA_IND"};
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
                anno.setGoName(nodes.getLbl());
                goLabelFound = true;
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

    void savePaintAnnotationsToSolr(String csv_path, String go_basic_path, SolrClient solrClient, String solr_collection) throws Exception {
        GoBasic go_basic = Util.loadJsonFromFile(go_basic_path);

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
                    ObjectWriter ow = new ObjectMapper().writer();
                    String goAnnotationDataStr = ow.writeValueAsString(anno);
                    doc.addField("go_annotations", goAnnotationDataStr);
                    solrClient.add("paint_db", doc);
                    solrClient.commit("paint_db");
                }
            }
            System.out.println(count);
        }
    }

    public static void main(String args[]) throws Exception {
        PaintServerWrapper server = new PaintServerWrapper();
    }
}
