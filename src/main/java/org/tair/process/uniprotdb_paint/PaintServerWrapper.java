package org.tair.process.uniprotdb_paint;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
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
        if (cols.length < 3)
            return null;
        PaintAnno anno = new PaintAnno();
        // UniprotId
        String uniprotId = cols[0].split("UniProtKB=")[1];
        anno.setGeneProductId(uniprotId);
        // Evidence Code
        String evidence_code = cols[2];
        anno.setEvidenceCode(evidence_code);
        // GoId
        anno.setGoId(cols[1]);
        String go_id = cols[1].split("GO:")[1];
        // dbReference
        String main_db = "PMID";
        String reference_id = "";
        if (cols.length == 4) {
            reference_id = cols[3];
        } else if (cols.length == 5) {
            reference_id = cols[4];
        }

        if (reference_id.contains("|")) {
            // Multiple references found!
            String[] reference_db_list = reference_id.split("\\|");
            boolean matchedMainRefDb = false;
            // Find if there is a main ref match
            for (int i = 0; i < reference_db_list.length; i++) {
                String ref_db = reference_db_list[i];
                String ref_db_name = ref_db.split(":")[0];
                if (ref_db_name.equals(main_db)) {
                    reference_id = ref_db;
                    matchedMainRefDb = true;
                    break;
                }
            }
            if (!matchedMainRefDb) {
                System.out.println("Ref db did not match " + reference_id);
            }
        }
        anno.setReference(reference_id);

        // GoName, goAspect
        boolean goLabelFound = false;
        for (int i = 0; i < go_basic.getNodes().size(); i++) {
            Nodes nodes = go_basic.getNodes().get(i);
            if (nodes.getGoId() != null && nodes.getGoId().equals(go_id)) {
                anno.setGoName(nodes.getLbl());
                goLabelFound = true;
                if (nodes.getGoAspects().contains("molecular_function")) {
                    anno.setGoAspect("molecular_function");
                } else if (nodes.getGoAspects().contains("biological_process")) {
                    anno.setGoAspect("biological_process");
                }
            }
        }
        if (!goLabelFound) {
            System.out.println("Go Label not found!");
        }

        return anno;
    }

    // Added on 08/13/2021
    // Total records in paint 18011627, not added: 4457000+13125202
    // Total Docs in solr: 856466

    // Added on 03/15/2021

    // Added on 03/16/2021
    // Last processed: 14176000, added 560070

    // Added on 10/31/2022
    // Completed: 18837000, added 695925, not - 17185076
    void savePaintAnnotationsToSolr(String csv_path, String go_basic_path, SolrClient solrClient,
            String solr_collection) throws Exception {
        GoBasic go_basic = Util.loadJsonFromFile(go_basic_path);

        String[] evidence_codes = new String[] { "EXP", "IDA", "IEP", "IGI", "IMP", "IPI" };

        int last_processed = 956000;
        try (BufferedReader br = new BufferedReader(new FileReader(csv_path))) {
            String line;
            int count = 0;
            int notAdded_count = 0;
            while ((line = br.readLine()) != null) {
                if (count < last_processed) {
                    count = count + 1;
                    continue;
                }
                String[] cols = line.split("\\s+");
                // String uniprotId = cols[0].split("UniProtKB=")[1];
                boolean matchedEvidence = Arrays.asList(evidence_codes).contains(cols[2]);
                if (matchedEvidence) {
                    // System.out.println(line);
                    PaintAnno anno = processPaintAnno(cols, go_basic);
                    if (anno == null) {
                        System.out.println("Anno null " + line);
                        notAdded_count++;
                        continue;
                    }
                    // System.out.println("Loaded " + count);
                    SolrInputDocument doc = new SolrInputDocument();
                    doc.addField("uniprot_id", anno.getGeneProductId());
                    ObjectWriter ow = new ObjectMapper().writer();
                    String goAnnotationDataStr = ow.writeValueAsString(anno);
                    doc.addField("go_annotations", goAnnotationDataStr);
                    solrClient.add(solr_collection, doc);
                } else {
                    // System.out.println("Not added "+ count + " " + cols[2]);
                    notAdded_count++;
                }
                if (count % 1000 == 0) {
                    System.out.println("Processed " + count);
                    System.out.println("Not added " + notAdded_count);
                    solrClient.commit(solr_collection);
                }
                count = count + 1;
            }
            System.out.println("Total records in paint " + count);
            System.out.println("Total records added to solr " + (count - notAdded_count));
        }
    }

    public static void main(String args[]) throws Exception {

    }
}
