/***
 * Load go annotations from api into solr's uniprot_db collection
 ***/

package org.tair.process.paint;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.tair.module.GOAnnotation;
import org.tair.module.GOAnnotationData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class GOAnnotationPaintETLPipeline {

    private String solrUrl = "http://localhost:8983/solr";
    private SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
    private PaintServerWrapper paintServerWrapper = new PaintServerWrapper();
//    private GOAnnotationUrlToJson GOAnnotationUrlToJson = new GOAnnotationUrlToJson();
//    private static String GO_IBA_RESOURCES_DIR = "src/main/resources/panther/GO_IBA_annotations";
//    private static String GO_IBA_LOGS_DIR = "src/main/logs/uniprot_db";

    public void storeGOAnnotationFromPaintApiToPersistentIdDb() throws Exception {
        // remove all data from this collection
//        solrClient.deleteByQuery("uniprot_db", "*:*");

        final SolrQuery query = new SolrQuery("*:*");
        query.setRows(0);
        query.setFacet(true);
        query.addFacetField("persistent_ids");
        query.setFacetLimit(-1); // -1 means unlimited

        final QueryResponse response = solrClient.query("panther", query);
        final FacetField persistent_ids = response.getFacetField("persistent_ids");
        List<Count> counts = persistent_ids.getValues();
        System.out.println("Total number to load: " + counts.size());
        int starting_idx = 18500;
        for(int i=starting_idx; i < counts.size(); i++) {
            if(i%100 == 0) {
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Saving "+ i);
            }
            String persId = counts.get(i).getName();
            paintServerWrapper.storeGoAnnosLocally(persId);
        }
//        int iter = counts.size() / 500 + 1;
//        for (int i = 0; i < iter; i++) {
//            List<String> uniprotIdList = new ArrayList<String>();
//            for (int j = i * 500; j < (i + 1) * 500; j++) {
//                if (j == counts.size())
//                    break;
//                uniprotIdList.add(counts.get(j).getName());
//            }
//            System.out.println("Loading: " + i * 500 + "-" + (i + 1) * 500);
//            List<GOAnnotationData> goAnnotations = GOAnnotationUrlToJson.readGOAnnotationUrlToObjectList(String.join(",", uniprotIdList));
//            if (goAnnotations.size() > 0) {
//                solrClient.addBeans("uniprot_db", goAnnotations);
//                solrClient.commit("uniprot_db");
//            }
//        }
//        System.out.println("finished commits to uniprot_db collection");

    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();
        GOAnnotationPaintETLPipeline paintPipeline = new GOAnnotationPaintETLPipeline();
        paintPipeline.storeGOAnnotationFromPaintApiToPersistentIdDb();
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }
}
