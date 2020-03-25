/*** 
 * Load go annotations from api into solr's uniprot_db collection
 ***/

package org.tair.process.uniprotdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.tair.module.ConnectedXref;
import org.tair.module.GOAnnotation;
import org.tair.module.GOAnnotationData;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class GOAnnotationETLPipeline {

    private String solrUrl = "http://localhost:8983/solr";
    private SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
    private ObjectMapper mapper = new ObjectMapper();
    private ObjectWriter ow = new ObjectMapper().writer();
    private GOAnnotationUrlToJson GOAnnotationUrlToJson = new GOAnnotationUrlToJson();

    public void storeGOAnnotationFromApiToUniprotDb() throws Exception {

        final SolrQuery query = new SolrQuery("*:*");
        query.setRows(0);
        query.setFacet(true);
        query.addFacetField("uniprot_ids");
        query.setFacetLimit(-1); // -1 means unlimited

        final QueryResponse response = solrClient.query("panther", query);
        final FacetField uniprot_ids = response.getFacetField("uniprot_ids");
        List<Count> counts = uniprot_ids.getValues();
        System.out.println("Total number to load: " + counts.size());

        int iter = counts.size() / 500 + 1;
        for (int i = 0; i < iter; i++) {
            List<String> uniprotIdList = new ArrayList<String>();
            for (int j = i * 500; j < (i + 1) * 500; j++) {
                if (j == counts.size())
                    break;
                uniprotIdList.add(counts.get(j).getName());
            }
            System.out.println("Loading: " + i * 500 + "-" + (i + 1) * 500);
            List<GOAnnotationData> goAnnotations = GOAnnotationUrlToJson.readGOAnnotationUrlToObjectList(String.join(",", uniprotIdList));
            if (goAnnotations.size() > 0) {
                solrClient.addBeans("uniprot_db", goAnnotations);
                solrClient.commit("uniprot_db");
            }
        }
        System.out.println("finished commits to uniprot_db collection");

    }

    public void storeGOAnnotationFromFileToUniprotDb() throws Exception {
        try (
            BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/panther/GO_IBA_annotations/errorsLoadingGoAnnotationsFromFile.txt"));
            InputStream input = new FileInputStream("src/main/resources/panther/GO_IBA_annotations/goidname.properties");
        ) {
            // load props for goName field
            Properties prop = new Properties();
            prop.load(input);

            // get all gaf files in folder
            File[] files = new File("src/main/resources/panther/GO_IBA_annotations/").listFiles(file -> file.toString().endsWith(".gaf"));
            for (File file : files) {
                System.out.println("Processing: " + file);
                try(
                    FileReader fileReader = new FileReader(file);
                    BufferedReader br = new BufferedReader(fileReader);
                ){
                    String thisLine = null;
                    // skip first 4 lines
                    for (int i = 0; i < 4; i++) {
                        br.readLine();
                    }

                    List<GOAnnotationData> goAnnotations = new ArrayList<>();
                    int line = 5;
                    while ((thisLine = br.readLine()) != null) {
                        //			System.out.println("processing line: " + line);
                        String[] attributes = thisLine.split("\t");
                        // filters
                        if (attributes[3].equals("NOT")) {
                            writer.write("Filtered qualifier.\nline " + line++ + ", file " + file.getName() + "\nline content: " + Arrays.toString(attributes) + "\n");
                            continue;
                        }
                        if (!attributes[8].equals("F") && !attributes[8].equals("P")) {
                            writer.write("Filtered GO ascpect.\nline " + line++ + ", file " + file.getName() + "\nline content: " + Arrays.toString(attributes) + "\n");
                            continue;
                        }
                        // validators
                        if (!attributes[4].startsWith("GO:")) {
                            writer.write("Irregular GO id.\nline " + line++ + ", file " + file.getName() + "\nline content: " + Arrays.toString(attributes) + "\n");
                            continue;
                        }
                        if (!attributes[6].equals("IBA")) {
                            writer.write("Irregular evidence code.\nline " + line++ + ", file " + file.getName() + "\nline content: " + Arrays.toString(attributes) + "\n");
                            continue;
                        }
                        if (!attributes[10].startsWith("UniProtKB")) {
                            writer.write("Irregular uniprot id.\nline " + line++ + ", file " + file.getName() + "\nline content: " + Arrays.toString(attributes) + "\n");
                            continue;
                        }

                        GOAnnotation goAnnotation = new GOAnnotation();
                        String geneProductId = attributes[10].substring(10).split("\\|")[0].toLowerCase();
                        goAnnotation.setGeneProductId(geneProductId);
                        String goId = attributes[4];
                        goAnnotation.setGoId(goId);
                        String key = goId.split(":")[1];
                        goAnnotation.setGoName(prop.getProperty(key));
                        goAnnotation.setReference(attributes[5]);
                        goAnnotation.setEvidenceCode("ECO:0000318," + attributes[6] + ",phylogeny");
                        String withFrom = attributes[7];
                        List<ConnectedXref> connectedXrefs = new ArrayList<>();
                        for (String entry : withFrom.split("\\|")) {
                            String db = entry.split(":")[0];
                            String id = entry.substring(db.length() + 1);
                            ConnectedXref connectedXref = new ConnectedXref();
                            connectedXref.setDb(db);
                            connectedXref.setId(id);
                            connectedXrefs.add(connectedXref);
                        }
                        goAnnotation.setWithFrom(connectedXrefs);

                        GOAnnotationData goAnnotationData = new GOAnnotationData();
                        goAnnotationData.setUniprot_id(geneProductId);
                        goAnnotationData.setGo_annotations(ow.writeValueAsString(goAnnotation));
                        goAnnotations.add(goAnnotationData);
                        System.out.println(goAnnotationData.toString());
                            if (goAnnotations.size() >=500) {
                                solrClient.addBeans("uniprot_db",goAnnotations);
                                solrClient.commit("uniprot_db");
                                goAnnotations = new ArrayList<>();
                            }
                        line++;
                    }
                    // commit remaining annotations
                    solrClient.addBeans("uniprot_db",goAnnotations);
                    solrClient.commit("uniprot_db");
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();

        GOAnnotationETLPipeline goAnnotationETLPipeline = new GOAnnotationETLPipeline();

        // uncomment based on your needs
//        goAnnotationETLPipeline.storeGOAnnotationFromApiToUniprotDb();
//        goAnnotationETLPipeline.storeGOAnnotationFromFileToUniprotDb();

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Execution time in nanoseconds  : " + timeElapsed);
        System.out.println("Execution time in milliseconds : " +
                timeElapsed / 1000000);
    }
}
