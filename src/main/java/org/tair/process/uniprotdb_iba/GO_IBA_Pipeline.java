/*** 
 * Load mapping of go annotations from Panther's IBA GAF and uniprotID into solr's uniprot_db collection
 ***/

package org.tair.process.uniprotdb_iba;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.tair.module.GOAnnotation;
import org.tair.module.GOAnnotationData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//https://conf.arabidopsis.org/pages/viewpage.action?spaceKey=PHYL&title=GO+IBA+annotations
public class GO_IBA_Pipeline {
    private String RESOURCES_DIR = "src/main/resources";
    // Change resources base to your local resources panther folder or load it from
    // application.properties file.
    private String RESOURCES_BASE = "panther_resources";

    private String BASE_SOLR_URL = "http://localhost:8983/solr";
    // Solr Collection (Make sure this collection is added to your solr database)
    private String solr_collection = "uniprot_db";
    private SolrClient solrClient = null;

    // path where to save the processed gaf files
    private String GO_IBA_RESOURCES_DIR = "";
    private static String GO_IBA_LOGS_DIR = "";

    // External URLs to download
    private String GO_IBA_GAF_FTP_URL = "ftp://ftp.pantherdb.org/downloads/paint/presubmission";
    private String GO_OBO_URL = "http://current.geneontology.org/ontology/go-basic.obo";

    public GO_IBA_Pipeline() {
        loadProps();
        solrClient = new HttpSolrClient.Builder(BASE_SOLR_URL).build();
        GO_IBA_RESOURCES_DIR = RESOURCES_BASE + "/iba/";
        makeDir(GO_IBA_RESOURCES_DIR);
        GO_IBA_LOGS_DIR = GO_IBA_RESOURCES_DIR + "/logs/";
        makeDir(GO_IBA_LOGS_DIR);
        System.out.println("~~~~~~~~~~ GO_IBA_Pipeline ~~~~~~~~~~");
        System.out.println("RESOURCES_BASE: " + RESOURCES_BASE);
        System.out.println("GO_IBA_RESOURCES_DIR: " + GO_IBA_RESOURCES_DIR);
        System.out.println("BASE_SOLR_URL: " + BASE_SOLR_URL);
        System.out.println("GO_IBA_GAF_FTP_URL: " + GO_IBA_GAF_FTP_URL);
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
            if (prop.containsKey("RESOURCES_BASE")) {
                RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
            }
            if (prop.containsKey("BASE_SOLR_URL")) {
                BASE_SOLR_URL = prop.getProperty("BASE_SOLR_URL");
            }
            if (prop.containsKey("GO_IBA_GAF_FTP_URL")) {
                GO_IBA_GAF_FTP_URL = prop.getProperty("GO_IBA_GAF_FTP_URL");
            }
            if (prop.containsKey("GO_OBO_URL")) {
                GO_OBO_URL = prop.getProperty("GO_OBO_URL");
            }
        } catch (Exception e) {
            System.out.println("Prop file not found!");
        }
    }

    private void saveGAFResourcesLocally(GOAnnotationGafUtils goAnnotationGafUtils) throws Exception {
        // prepare for resources
        File iba_folder = new File(GO_IBA_RESOURCES_DIR);
        FileUtils.forceMkdir(iba_folder);
        FileUtils.cleanDirectory(iba_folder);
        goAnnotationGafUtils.loadGoIbaAnnotationsResources(GO_IBA_RESOURCES_DIR,
                GO_IBA_GAF_FTP_URL, GO_OBO_URL);
        System.out.println("~~~~~~~~~~~~ Loading IBA Resources Completed");
        goAnnotationGafUtils.generatePropFromObo(GO_IBA_RESOURCES_DIR);
        System.out.println("~~~~~~~~~~~~ Generate Prop From OBO Completed");
    }

    public void downloadIBAFilesLocally() throws Exception {
        GOAnnotationGafUtils goAnnotationGafUtils = new GOAnnotationGafUtils();
        saveGAFResourcesLocally(goAnnotationGafUtils);
    }

    /***
     * Load go annotations from gaf file into solr's uniprot_db collection by the
     * following steps:
     * 1. prepare for resources including gaf file, obo file, properties file
     * 2. traverse all gaf files and read go annotation data
     * 3. commit go annotation data in batch to uniprot_db collection
     ***/
    public void updateIBAGOFromLocalToSolr(Boolean clearSolr) throws Exception {
        if (clearSolr) {
            // WARNING: remove all data from this collection, make sure you have backup
            solrClient.deleteByQuery(solr_collection, "*:*");
            solrClient.commit(solr_collection);
            System.out.println("removed all solr from " + solr_collection);
        }

        GOAnnotationGafUtils goAnnotationGafUtils = new GOAnnotationGafUtils();
        int totalLines = 0;
        try (
                BufferedWriter writer = new BufferedWriter(
                        new FileWriter(GO_IBA_LOGS_DIR + "/errorsLoadingGoAnnotationsFromFile.log"));
                InputStream input = new FileInputStream(GO_IBA_RESOURCES_DIR + "/goidname.properties");) {
            // load props for goName field
            Properties prop = new Properties();
            prop.load(input);

            // get all gaf files in folder
            File[] files = new File(GO_IBA_RESOURCES_DIR).listFiles(file -> file.toString().endsWith(".gaf"));
            for (File file : files) {
                System.out.println("Processing: " + file);
                try (
                        FileReader fileReader = new FileReader(file);
                        BufferedReader br = new BufferedReader(fileReader);) {
                    String thisLine = null;
                    List<GOAnnotationData> goAnnotations = new ArrayList<>();
                    int lineNum = 1;
                    while ((thisLine = br.readLine()) != null) {
                        // validate line
                        String validateMsg = goAnnotationGafUtils.validateGafLine(thisLine);
                        if (!validateMsg.equals("Valid")) {
                            writer.write(validateMsg + "\nline " + lineNum++ + ", file " + file.getName()
                                    + "\nline content: " + thisLine + "\n");
                            continue;
                        }
                        // read from line and create GOAnnotation obj
                        GOAnnotation goAnnotation = GOAnnotation.readFromGafLine(thisLine, prop);
                        if (goAnnotation == null) {
                            continue;
                        }
                        // create GOAnnotationData obj from GOAnnotation obj
                        GOAnnotationData goAnnotationData = GOAnnotationData.createFromGOAnnotation(goAnnotation);

                        goAnnotations.add(goAnnotationData);
                        // System.out.println(goAnnotationData.toString());
                        if (goAnnotations.size() >= 500) {
                            System.out.println("processed: " + lineNum);
                            solrClient.addBeans("uniprot_db", goAnnotations);
                            solrClient.commit("uniprot_db");
                            goAnnotations = new ArrayList<>();
                        }
                        lineNum++;
                    }
                    // commit remaining annotations
                    solrClient.addBeans("uniprot_db", goAnnotations);
                    solrClient.commit("uniprot_db");
                    System.out.println("processed: " + lineNum);
                    totalLines += lineNum;
                }
            }
        }
        System.out.println("finished adding data to uniprot_db collection from gaf files. totalLines " + totalLines);
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();

        GO_IBA_Pipeline ibaPipeline = new GO_IBA_Pipeline();
        // uncomment based on your needs
        // important: if the url of gaf file or obo file changes, we need to update them
        // in applications.properties file, otherwise it may not reflect the correct
        // data;
        // if the format of gaf file or obo file has been changed, we need to change the
        // code accordingly.
        // "loadResources": to true if the GAF files for IBA annotations are not in the
        // resources folder, or want to redownload it.
        // last process on 04/26/2021: totalLines 3981673
        // last process on 08/14/2021: totalLines 3894973
        // ibaPipeline.updateGOAnnotationFromFileToUniprotDb(false);

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }

    /***
     * Old code for loading GO annotations to "uniprot_db" from the api. This is
     * changed to load from paint instead.
     * 
     * @throws Exception
     */
    public void storeGOAnnotationFromApiToUniprotDb() throws Exception {
        // remove all data from this collection
        // solrClient.deleteByQuery("uniprot_db", "*:*");
        //
        // final SolrQuery query = new SolrQuery("*:*");
        // query.setRows(0);
        // query.setFacet(true);
        // query.addFacetField("uniprot_ids");
        // query.setFacetLimit(-1); // -1 means unlimited
        //
        // final QueryResponse response = solrClient.query("panther", query);
        // final FacetField uniprot_ids = response.getFacetField("uniprot_ids");
        // List<Count> counts = uniprot_ids.getValues();
        // System.out.println("Total number to load: " + counts.size());
        //
        // int iter = counts.size() / 500 + 1;
        // for (int i = 0; i < iter; i++) {
        // List<String> uniprotIdList = new ArrayList<String>();
        // for (int j = i * 500; j < (i + 1) * 500; j++) {
        // if (j == counts.size())
        // break;
        // uniprotIdList.add(counts.get(j).getName());
        // }
        // System.out.println("Loading: " + i * 500 + "-" + (i + 1) * 500);
        // List<GOAnnotationData> goAnnotations =
        // GOAnnotationUrlToJson.readGOAnnotationUrlToObjectList(String.join(",",
        // uniprotIdList));
        // if (goAnnotations.size() > 0) {
        // solrClient.addBeans("uniprot_db", goAnnotations);
        // solrClient.commit("uniprot_db");
        // }
        // }
        // System.out.println("finished commits to uniprot_db collection");

    }

}
