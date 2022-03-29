/***
 * Load mapping of go annotations from PAINT url and uniprotID into solr's paint_db collection.
 * "go_basic.json": Is used to
 ***/

package org.tair.process.uniprotdb_paint;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class GO_PAINT_Pipeline {
    private String RESOURCES_DIR = "src/main/resources";
    // Change resources base to your local resources panther folder or load it from
    // application.properties file.
    private String RESOURCES_BASE = "panther_resources";

    private String BASE_SOLR_URL = "http://localhost:8983/solr";
    // Solr Collection (Make sure this collection is added to your solr database)
    private String solr_collection = "paint_db";

    private SolrClient solrClient = null;

    // path where to save the downloaded tsv files
    private String GO_PAINT_RESOURCES_DIR = "";
    // path where the script to download TSV from the panther ftp url
    private static String GO_PAINT_SCRIPTS_PATH = "src/main/scripts/GO_PAINT_annotations";
    // External URLs to download
    private String GO_PAINT_TSV_FTP_URL = "";
    private String GO_OBO_URL = "http://current.geneontology.org/ontology/go-basic.obo";

    private PaintServerWrapper paintServerWrapper = new PaintServerWrapper();

    public GO_PAINT_Pipeline() {
        loadProps();
        solrClient = new HttpSolrClient.Builder(BASE_SOLR_URL).build();
        GO_PAINT_RESOURCES_DIR = RESOURCES_BASE + "/paint/";
        makeDir(GO_PAINT_RESOURCES_DIR);
        System.out.println("~~~~~~~~~~ GO_PAINT_Pipeline ~~~~~~~~~~");
        System.out.println("RESOURCES_BASE: " + RESOURCES_BASE);
        System.out.println("BASE_SOLR_URL: " + BASE_SOLR_URL);
        System.out.println("GO_PAINT_TSV_FTP_URL: " + GO_PAINT_TSV_FTP_URL);
        System.out.println("GO_OBO_URL: " + GO_OBO_URL);
    }

    public void makeDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.isDirectory()) {
            dir.mkdir();
            System.out.println("Making dir " + dirPath);
        }
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
            if (prop.containsKey("BASE_SOLR_URL")) {
                BASE_SOLR_URL = prop.getProperty("BASE_SOLR_URL");
            }
            if (prop.containsKey("GO_PAINT_TSV_FTP_URL")) {
                GO_PAINT_TSV_FTP_URL = prop.getProperty("GO_PAINT_TSV_FTP_URL");
            }
            if (prop.containsKey("GO_OBO_URL")) {
                GO_OBO_URL = prop.getProperty("GO_OBO_URL");
            }
        } catch (Exception e) {
            System.out.println("Prop file not found!");
        }
    }

    public void downloadPAINTFilesLocally() throws Exception {
        ProcessBuilder pb = new ProcessBuilder("./resources.sh",
                GO_PAINT_RESOURCES_DIR,
                GO_PAINT_TSV_FTP_URL, GO_OBO_URL);
        pb.directory(new File(GO_PAINT_SCRIPTS_PATH));
        pb.inheritIO(); // print script output to console
        Process p = pb.start();
        p.waitFor(); // wait until script finish
    }

    public void updatePAINTGOFromLocalToSolr(Boolean clearSolr) throws Exception {
        if (clearSolr) {
            // WARNING: remove all data from this collection, make sure you have backup
            solrClient.deleteByQuery(solr_collection, "*:*");
            solrClient.commit(solr_collection);
            System.out.println("cleared all solr data from " + solr_collection);
        }

        // get tsv file
        File[] files = new File(GO_PAINT_RESOURCES_DIR).listFiles(file -> file.toString().endsWith(".tsv"));
        File tsvFile = null;
        if (files.length == 1) {
            tsvFile = files[0];
            System.out.println("Found TSV File: " + tsvFile.getName());
        } else {
            if (files.length == 0) {
                System.out.println("No PAINT TSV file found at the GO_PAINT_RESOURCES_DIR");
            } else {
                System.out.println("Only one TSV file must be present at the GO_PAINT_RESOURCES_DIR");
            }
            return;
        }

        // get tsv file
        files = new File(GO_PAINT_RESOURCES_DIR).listFiles(file -> file.toString().endsWith(".json"));
        File jsonFile = null;
        if (files.length == 1) {
            jsonFile = files[0];
            System.out.println("Found JSON File: " + jsonFile.getName());
        } else {
            if (files.length == 0) {
                System.out.println("No GO BASIC JSON file found at the GO_PAINT_RESOURCES_DIR");
            } else {
                System.out.println("Only one JSON file must be present at the GO_PAINT_RESOURCES_DIR");
            }
            return;
        }

        paintServerWrapper.savePaintAnnotationsToSolr(tsvFile.getAbsolutePath(), jsonFile.getAbsolutePath(), solrClient,
                solr_collection);
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();
        GO_PAINT_Pipeline paintPipeline = new GO_PAINT_Pipeline();
        // paintPipeline.loadIBAAnnotations();

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }
}
