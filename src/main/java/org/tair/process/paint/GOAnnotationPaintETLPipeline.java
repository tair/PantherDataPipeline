/***
 * Load go annotations from paint csv file into solr's paint_db collection.
 * "go_basic.json": Is used to
 ***/

package org.tair.process.paint;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class GOAnnotationPaintETLPipeline {
    private String RESOURCES_DIR = "src/main/resources";
    //Change resources base to your local resources panther folder or load it from application.properties file.
    private String RESOURCES_BASE = "panther_resources";

    private String BASE_SOLR_URL = "http://localhost:8983/solr";
    //Solr Collection (Make sure this collection is added to your solr database)
    private String solr_collection = "paint_db";

    private SolrClient solrClient = null;

    private String PAINT_TSV_NAME = "Pthr_GO_15.0.tsv";
    private String GO_BASIC_NAME = "go-basic.json";


    private PaintServerWrapper paintServerWrapper = new PaintServerWrapper();

    public GOAnnotationPaintETLPipeline() {
        loadProps();
        solrClient = new HttpSolrClient.Builder(BASE_SOLR_URL).build();
    }

    public void makeDir(String dirPath) {
        File dir = new File(dirPath);
        if(!dir.isDirectory()) {
            dir.mkdir();
            System.out.println("Making dir "+ dirPath);
        }
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
            if(prop.containsKey("BASE_SOLR_URL")) {
                BASE_SOLR_URL= prop.getProperty("BASE_SOLR_URL");
            }
            if(prop.containsKey("PAINT_TSV_NAME")) {
                PAINT_TSV_NAME= prop.getProperty("PAINT_TSV_NAME");
            }
            if(prop.containsKey("GO_BASIC_NAME")) {
                GO_BASIC_NAME= prop.getProperty("GO_BASIC_NAME");
            }
        } catch (Exception e) {
            System.out.println("Prop file not found!");
        }
    }

    public void loadPaintAnnotations() throws Exception {
        String csv_path = RESOURCES_BASE + "/" + PAINT_TSV_NAME;
        File f = new File(csv_path);
        if(!f.exists()) {
            System.out.println("PAINT_TSV_NAME does not exist at " + csv_path);
        }
        String go_basic_path = RESOURCES_BASE + "/" + GO_BASIC_NAME;
        f = new File(go_basic_path);
        if(!f.exists()) {
            System.out.println("GO_BASIC_NAME does not exist at " + go_basic_path);
        }
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
