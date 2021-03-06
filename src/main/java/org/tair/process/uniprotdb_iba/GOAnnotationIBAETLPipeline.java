/*** 
 * Load go annotations from api into solr's uniprot_db collection
 ***/

package org.tair.process.uniprotdb_iba;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.tair.module.GOAnnotation;
import org.tair.module.GOAnnotationData;
import org.tair.process.paint.GOAnnotationPaintETLPipeline;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//https://conf.arabidopsis.org/pages/viewpage.action?spaceKey=PHYL&title=GO+IBA+annotations
public class GOAnnotationIBAETLPipeline {
    private String RESOURCES_DIR = "src/main/resources";
    //Change resources base to your local resources panther folder or load it from application.properties file.
    private String RESOURCES_BASE = "panther_resources";

    private String BASE_SOLR_URL = "http://localhost:8983/solr";
    //Solr Collection (Make sure this collection is added to your solr database)
    private String solr_collection = "uniprot_db";
    private SolrClient solrClient = null;

    //path where to save the processed gaf files
    private String GO_IBA_RESOURCES_DIR = "src/main/resources/panther16/GO_IBA_annotations";
    private static String GO_IBA_LOGS_DIR = "src/main/logs/uniprot_db";


    public GOAnnotationIBAETLPipeline() {
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
            if(prop.containsKey("RESOURCES_BASE")) {
                RESOURCES_BASE = prop.getProperty("RESOURCES_BASE");
                makeDir(RESOURCES_BASE);
            }
            if(prop.containsKey("BASE_SOLR_URL")) {
                BASE_SOLR_URL= prop.getProperty("BASE_SOLR_URL");
            }
        } catch(Exception e) {
            System.out.println("Prop file not found!");
        }
    }

    /***
     * Old code for loading GO annotations to "uniprot_db" from the api. This is changed to load from paint instead.
     * @throws Exception
     */
    public void storeGOAnnotationFromApiToUniprotDb() throws Exception {
        // remove all data from this collection
//        solrClient.deleteByQuery("uniprot_db", "*:*");
//
//        final SolrQuery query = new SolrQuery("*:*");
//        query.setRows(0);
//        query.setFacet(true);
//        query.addFacetField("uniprot_ids");
//        query.setFacetLimit(-1); // -1 means unlimited
//
//        final QueryResponse response = solrClient.query("panther", query);
//        final FacetField uniprot_ids = response.getFacetField("uniprot_ids");
//        List<Count> counts = uniprot_ids.getValues();
//        System.out.println("Total number to load: " + counts.size());
//
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

    private void saveGAFResourcesLocally(GOAnnotationGafUtils goAnnotationGafUtils) throws Exception {
        // prepare for resources
        FileUtils.cleanDirectory(new File(GO_IBA_RESOURCES_DIR));
        goAnnotationGafUtils.loadGoIbaAnnotationsResources(GO_IBA_RESOURCES_DIR);
        goAnnotationGafUtils.generatePropFromObo(GO_IBA_RESOURCES_DIR);
    }
    /***
     * Load go annotations from gaf file into solr's uniprot_db collection by the following steps:
     * 1. prepare for resources including gaf file, obo file, properties file
     * 2. traverse all gaf files and read go annotation data
     * 3. commit go annotation data in batch to uniprot_db collection
     ***/
    public void updateGOAnnotationFromFileToUniprotDb(Boolean loadResources) throws Exception {
        // remove all data from this collection
        solrClient.deleteByQuery("uniprot_db", "*:*");

        GOAnnotationGafUtils goAnnotationGafUtils = new GOAnnotationGafUtils();
//        if(loadResources) {
//            saveGAFResourcesLocally(goAnnotationGafUtils);
//        }
        int totalLines = 0;
        try (
                BufferedWriter writer = new BufferedWriter(new FileWriter(GO_IBA_LOGS_DIR + "/errorsLoadingGoAnnotationsFromFile.log"));
                InputStream input = new FileInputStream(GO_IBA_RESOURCES_DIR + "/goidname.properties");
        ) {
            // load props for goName field
            Properties prop = new Properties();
            prop.load(input);

            // get all gaf files in folder
            File[] files = new File(GO_IBA_RESOURCES_DIR).listFiles(file -> file.toString().endsWith(".gaf"));
            for (File file : files) {
                System.out.println("Processing: " + file);
                try (
                        FileReader fileReader = new FileReader(file);
                        BufferedReader br = new BufferedReader(fileReader);
                ) {
                    String thisLine = null;
                    List<GOAnnotationData> goAnnotations = new ArrayList<>();
                    int lineNum = 1;
                    while ((thisLine = br.readLine()) != null) {
                        // validate line
                        String validateMsg = goAnnotationGafUtils.validateGafLine(thisLine);
                        if (!validateMsg.equals("Valid")) {
                            writer.write(validateMsg + "\nline " + lineNum++ + ", file " + file.getName() + "\nline content: " + thisLine + "\n");
                            continue;
                        }
                        // read from line and create GOAnnotation obj
                        GOAnnotation goAnnotation = GOAnnotation.readFromGafLine(thisLine, prop);
                        if(goAnnotation == null) {
                            continue;
                        }
                        // create GOAnnotationData obj from GOAnnotation obj
                        GOAnnotationData goAnnotationData = GOAnnotationData.createFromGOAnnotation(goAnnotation);

                        goAnnotations.add(goAnnotationData);
//                        System.out.println(goAnnotationData.toString());
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
        //First load all the paint annotations to "paint_db" solr
//        GOAnnotationPaintETLPipeline paintPipeline = new GOAnnotationPaintETLPipeline();
//        paintPipeline.loadPaintAnnotations();

        GOAnnotationIBAETLPipeline ibaPipeline = new GOAnnotationIBAETLPipeline();
        // uncomment based on your needs
        // important: if the url of gaf file or obo file changes, we need to update them in applications.properties file, otherwise it may not reflect the correct data;
        // if the format of gaf file or obo file has been changed, we need to change the code accordingly.
        // "loadResources": to true if the GAF files for IBA annotations are not in the resources folder, or want to redownload it.
        ibaPipeline.updateGOAnnotationFromFileToUniprotDb(true);

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }
}
