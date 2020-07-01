/***
 * Load go annotations from paint csv file into solr's paint_db collection.
 * "go_basic.json": Is used to
 ***/

package org.tair.process.paint;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class GOAnnotationPaintETLPipeline {

    private String solrUrl = "http://localhost:8983/solr";
    private SolrClient solrClient = null;
    //Change resources base to your local resources panther folder
    private String RESOURCES_DIR = "src/main/resources";
    private String PAINT_TSV_NAME = "Pthr_GO_14.1.tsv";
    private String GO_BASIC_NAME = "go-basic.json";
    //Solr Collection (Make sure this collection is added to your solr database)
    private String solr_collection = "paint_db";

    private PaintServerWrapper paintServerWrapper = new PaintServerWrapper();

    public GOAnnotationPaintETLPipeline() {
        solrClient = new HttpSolrClient.Builder(solrUrl).build();
    }

    public void loadPaintAnnotations() throws Exception {
        String csv_path = RESOURCES_DIR + "/" + PAINT_TSV_NAME;
        String go_basic_path = RESOURCES_DIR + "/" + GO_BASIC_NAME;
        paintServerWrapper.savePaintAnnotationsToSolr(csv_path, go_basic_path, solrClient, solr_collection);
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();
        GOAnnotationPaintETLPipeline paintPipeline = new GOAnnotationPaintETLPipeline();
        paintPipeline.loadPaintAnnotations();

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }
}
