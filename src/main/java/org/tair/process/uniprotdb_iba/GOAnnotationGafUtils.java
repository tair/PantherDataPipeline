package org.tair.process.uniprotdb_iba;

import com.amazonaws.services.dynamodbv2.xspec.S;

import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;

public class GOAnnotationGafUtils {

    private static String RESOURCES_DIR = "src/main/resources";
    private static String GO_IBA_SCRIPTS_PATH = "src/main/scripts/GO_IBA_annotations";

    // download gaf files and obo files
    public void loadGoIbaAnnotationsResources(String goIbaResourcesDir) throws Exception {
        try (
                InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
        ) {
            // load props
            Properties prop = new Properties();
            prop.load(input);
            // get relative url from script dir to resources dir for shell script to use
            System.out.println(goIbaResourcesDir);
            String relativePath = Paths.get(GO_IBA_SCRIPTS_PATH).relativize(Paths.get(goIbaResourcesDir)).toString();
            // build process
            ProcessBuilder pb = new ProcessBuilder("./resources.sh", relativePath, prop.getProperty("GoIbaAnnotationFtpUrl"), prop.getProperty("GoBasicOboUrl"));
            pb.directory(new File(GO_IBA_SCRIPTS_PATH));
            pb.inheritIO(); // print script output to console
            Process p = pb.start();
            p.waitFor(); // wait until script finish
        }
    }

    // generate properties file from obo file to get go id and go name mapping
    public void generatePropFromObo(String goIbaResourcePath) throws Exception {
        System.out.println("Generating properties file from obo file...");
        File file = new File(goIbaResourcePath + "/go-basic.obo");
        try (
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);
                BufferedWriter writer = new BufferedWriter(new FileWriter(goIbaResourcePath + "/goidname.properties"));
        ) {
            String thisLine = null;
            String goId = null;
            String goName = null;
            int lineNum = 1;
            while ((thisLine = br.readLine()) != null) {
                if (thisLine.startsWith("id: ")) {
                    if (goId == null) {
                        goId = thisLine.substring(4);
                    } else {
                        // if go id already assigned with value, throw error to prevent unknown overwriting
                        throw new Exception("Invalid input. Line number: " + lineNum);
                    }
                } else if (thisLine.startsWith("name: ")) {
                    if (goName == null) {
                        goName = thisLine.substring(6);
                    } else {
                        // if go name already assigned with value, throw error to prevent unknown overwriting
                        throw new Exception("Invalid input. Line number: " + lineNum);
                    }
                } else if (thisLine.startsWith("namespace: ")) {
                    if (goId != null && goName != null) {
                        writer.write(goId.substring(3) + '=' + goName + "\n");
                        goId = null;
                        goName = null;
                    } else {
                        // prevent null value from being assigned
                        throw new Exception("Invalid input. Line number: " + lineNum);
                    }
                } else if (thisLine.startsWith("[Typedef]")) {
                    break;
                }
                lineNum++;
            }
        }
        System.out.println("Finished generating properties file from obo file.");
    }

    // parse and validate one line of gaf file before it can be used
    public String validateGafLine(String lineStr) {
        if (lineStr.startsWith("!")) {
            return "Invalid line";
        }
        String[] attributes = lineStr.split("\t");
        // filters
        if (attributes[3].equals("NOT")) {
            return "Filtered qualifier";
        }
        if (!attributes[8].equals("F") && !attributes[8].equals("P")) {
            return "Filtered GO ascpect";
        }
        // validators
        if (!attributes[4].startsWith("GO:")) {
            return "Irregular GO id";
        }
        if (!attributes[6].equals("IBA")) {
            return "Irregular evidence code";
        }
        if (!attributes[10].startsWith("UniProtKB")) {
            return "Irregular uniprot id";
        }
        return "Valid";
    }
}
