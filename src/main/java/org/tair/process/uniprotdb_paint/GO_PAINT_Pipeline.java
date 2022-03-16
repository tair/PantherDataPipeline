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
        System.out.println("~~~~~~~~~~ GO_IBA_Pipeline ~~~~~~~~~~");
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
            System.out.println("removed all solr from " + solr_collection);
        }

        // get all gaf files in folder
        File[] files = new File(GO_PAINT_RESOURCES_DIR).listFiles(file -> file.toString().endsWith(".tsv"));
        if (files.length == 1) {
            File tsvFile = files[0];
            System.out.println("Found TSV File: " + tsvFile.getName());
        } else {
            if (files.length == 0) {
                System.out.println("No PAINT TSV file found at the GO_PAINT_RESOURCES_DIR");
            } else {
                System.out.println("Only one TSV file must be present at the GO_PAINT_RESOURCES_DIR");
            }
        }
        // String csv_path = GO_PAINT_RESOURCES_DIR + "/" + PAINT_TSV_NAME;
        // File f = new File(csv_path);
        // if (!f.exists()) {
        // System.out.println("PAINT_TSV_NAME does not exist at " + csv_path);
        // return;
        // }
        // String go_basic_path = RESOURCES_BASE + "/" + GO_BASIC_NAME;
        // f = new File(go_basic_path);
        // if (!f.exists()) {
        // System.out.println("GO_BASIC_NAME does not exist at " + go_basic_path);
        // return;
        // }
        // System.out.println("~~~~~~~~~~~~~~ Load IBA GO Annnotations from Paint DB
        // ~~~~~~~~~~~~~~~~");
        // System.out.println("PAINT_TSV_NAME: " + PAINT_TSV_NAME);
        // System.out.println("SOLR_COLLECTION: " + solr_collection);

        // paintServerWrapper.savePaintAnnotationsToSolr(csv_path, go_basic_path,
        // solrClient, solr_collection);
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
