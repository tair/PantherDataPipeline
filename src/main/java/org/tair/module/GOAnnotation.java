package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GOAnnotation {
    private String geneProductId;
    private String goId;
    private String goName;
    private String goAspect;

    private String evidenceCode;
    private String reference;
//    private List<ConnectedXref> withFrom;

    @JsonProperty("geneProductId")
    private void buildGeneProductId(String geneProductId) {
        if (geneProductId.startsWith("UniProtKB:")) {
            this.geneProductId = geneProductId.split(":")[1];
        } else {
            this.geneProductId = geneProductId;
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/resources/irregularGeneProductId.txt", true));
                writer.write(geneProductId + "\n");
                writer.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

//    @JsonProperty("withFrom")
//    private void buildWithFrom(List<WithFromItem> withFrom) {
//        if (withFrom != null) {
//            List<ConnectedXref> withFromList = new ArrayList<ConnectedXref>();
//            for (WithFromItem withFromItem : withFrom) {
//                List<ConnectedXref> connectedXrefs = withFromItem.getConnectedXrefs();
//                for (ConnectedXref connectedXref : connectedXrefs) {
//                    withFromList.add(connectedXref);
//                }
//            }
//            this.withFrom = withFromList;
//        } else {
//            this.withFrom = null;
//        }
//    }

    @JsonProperty("evidenceCode")
    private void buildEvidence(String evidenceCode) {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("src/main/resources/evidence.properties");

            // load a properties file
            prop.load(input);

            String key = evidenceCode.split(":")[1];
            this.evidenceCode = prop.getProperty(key);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // read data from a line of gaf file and return a GOAnnotation obj
    public static GOAnnotation readFromGafLine(String lineStr, Properties prop) {
        String[] attributes = lineStr.split("\t");
        GOAnnotation goAnnotation = new GOAnnotation();
        String geneProductId = attributes[10].substring(10).split("\\|")[0].toLowerCase();
        goAnnotation.setGeneProductId(geneProductId);
        String goId = attributes[4];
        goAnnotation.setGoId(goId);
        String key = goId.split(":")[1];
        goAnnotation.setGoName(prop.getProperty(key)); // get go name by go id from prop file
        goAnnotation.setReference(attributes[5]);
        // compose evidence code string for IBA, if in the future there are more evidence codes, this should be changed accordingly
        goAnnotation.setEvidenceCode("ECO:0000318," + attributes[6] + ",phylogeny");

        String goAspect = attributes[8];
        if(goAspect.equals("C")) {
            System.out.println("Ignore C");
            return null;
        }
        goAnnotation.setGoAspect(goAspect);
        return goAnnotation;
    }
}