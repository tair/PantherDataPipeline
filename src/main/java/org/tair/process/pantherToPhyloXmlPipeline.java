package org.tair.process;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.tair.module.PantherData;
//import org.tair.module.pantherForPhylo.*;
import org.tair.module.panther.*;
import org.tair.module.pantherForPhylo.Accession;
import org.tair.module.pantherForPhylo.familyList;
import org.tair.module.pantherForPhylo.FamilyNames;
import org.tair.module.phyloxml.*;
import org.tair.process.panther.PhylogenesServerWrapper;

import java.io.File;
import java.io.IOException;
import java.util.*;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.InputStream;
import java.io.FileInputStream;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.StreamResult;
/*
    Purpose:
        Input: place Panther Json Files in main/resources/panther/pruned_panther_files directory
        Output: receive Json files in xml format in main/resources/phyloxml directory

        There are some sample panther json files in there currently.
*/
public class pantherToPhyloXmlPipeline
{
    private String RESOURCES_DIR = "src/main/resources";
    public static String RESOURCES_BASE = "panther_resources";

    PhylogenesServerWrapper pgServer = new PhylogenesServerWrapper();

    public pantherToPhyloXmlPipeline() {
        loadProps();
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
            }
        } catch (Exception e) {
//            System.out.println("Prop file not found!");
        }
    }

    public static void main(String args[])
    {
        pantherToPhyloXmlPipeline pxml = new pantherToPhyloXmlPipeline();
//        String[] sel_ids = {"PTHR10177"};
//        List<String> sel_list = Arrays.asList(sel_ids);
//        for(int i=0; i<sel_list.size(); i++) {
//            PantherJsonToPhyloXml(sel_list.get(i));
//        }

//        convertAllInDirectory();   // converts all panther json files in pruned_panther_files directory

//        pxml.uploadAlltoS3();
//        pxml.uploadSelectedToS3();
    }

    // specifically converts all pruned pantherForPhylo files from resource directory
    // and places them into phyloxml directory
    static void convertAllInDirectory()
    {
        File dir = new File(RESOURCES_BASE + "/pruned_panther_files");
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null)
        {
            int fileCount = 0;
            for (File child : directoryListing)
            {
                if (child.getName().charAt(0) != '.') // to ignore files such as .gitignore and .ds_store
                    fileCount++;
                    PantherJsonToPhyloXml(child.getName().replace(".json", ""));
                    System.out.println("Saved "+ fileCount + " " + child.getName());
            }
        }
    }

    void uploadAlltoS3() {
        File dir = new File(RESOURCES_BASE + "/phyloXml");
        String bucketName = "phyloxml-16";
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            int fileCount = 0;
            for (File child : directoryListing) {
                if (child.getName().charAt(0) != '.') {// to ignore files such as .gitignore and .ds_store
                    fileCount++;
                    pgServer.uploadObjectToBucket(bucketName, child.getName(), child);
                    System.out.println("Saved S3: "+ fileCount + " " + child.getName());
                }
            }
        }
    }

    void uploadSelectedToS3() {
        File dir = new File(RESOURCES_BASE + "/phyloXml");
        String bucketName = "phyloxml-15";
        File[] directoryListing = dir.listFiles();
        String[] sel_ids = {"PTHR10177"};
        List<String> sel_list = Arrays.asList(sel_ids);
        if (directoryListing != null) {
            int fileCount = 0;
            for (File child : directoryListing) {
                if (child.getName().charAt(0) != '.') {// to ignore files such as .gitignore and .ds_store
                    fileCount++;
                    if(sel_list.contains(child.getName().replace(".xml", ""))) {
                        System.out.println("Saved S3: "+ fileCount + " " + child.getName());
                    }
//                    pgServer.uploadObjectToBucket(bucketName, child.getName(), child);
                }
            }
        }
    }

    // Converts a single pantherForPhylo Json to Phylo Xml file
    // only input file name without file extension
    // files r normally located in main/resources/pruned_panther_files, however
    // u can change path if json files placed elsewhere
    static void PantherJsonToPhyloXml(String fileName)
    {
        // declarations for mapping pantherForPhylo json objects to java objects
        ObjectMapper objectMapper = new ObjectMapper();
        PantherData panther = new PantherData();
        try
        {
            // mapping pantherForPhylo json file to pantherj java object
            panther = objectMapper.readValue(new File(RESOURCES_BASE + "/pruned_panther_files/"+fileName+".json"), PantherData.class);
            panther = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(panther.getJsonString(),
                    PantherData.class);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println(fileName);
//        System.out.println(panther.getSearch().getAnnotation_node().getSf_name());
        // some objects in the pruned files are empty with just an ID, this avoids creating an xml for it

        if (panther.getSearch() != null)
        {
//            // creating the root node and setting up transformation(pantherForPhylo -> phyloxml)
            Phyloxml phylo = createPhyloRoot(panther);
            // transformation pantherForPhylo object to phyloxml object, build phyloxml tree while level order traversal
            constructPhyloTreeWithAnnoTree(panther.getSearch().getAnnotation_node(), phylo.getPhylogeny().getClade());
            // transform phlyo java object to xml local file
            String xmlLocalFile_fullPath = RESOURCES_BASE + "/phyloXml/" + fileName+".xml";
            phyloObjToXML(phylo, xmlLocalFile_fullPath);
        }

    }

//    Creates the first node, and classes containing the node before
//    passing that root to a function that creates the tree.
    static Phyloxml createPhyloRoot(PantherData panther)
{
    Phyloxml phylo = new Phyloxml();
    phylo.setPhylogeny(new Phylogeny());
    phylo.getPhylogeny().setClade(new Clade());
    phylo.getPhylogeny().getClade().setEvents(new Events());
    phylo.getPhylogeny().setDescription(panther.getId());
    phylo.getPhylogeny().setRooted("true");                     // CHECK WHEN TO KNOW TRUE AND FALSE
    phylo.getPhylogeny().setName(panther.getFamily_name());
    return phylo;
}


//    Sets clade attributes equal to module.pantherForPhylo.Annotation attributes but only if they are needed so XML file looks correct.
//    So we only allocate memory for objects when we see the module.pantherForPhylo.Annotation node attributes are not empty.
//    This is a helper function for
    static void createAndSetCladeWithAno(Annotation anno, Clade clade)
    {
        if (anno.getEvent_type() != null)
        {
            clade.setEvents(new Events());
            if (anno.getEvent_type().equals("SPECIATION"))
                clade.getEvents().setSpeciations("1");
            if (anno.getEvent_type().equals("DUPLICATION"))
                clade.getEvents().setDuplications("1");
        }
        if (anno.getOrganism() != null)
        {
            clade.setTaxonomy(new Taxonomy());
            clade.getTaxonomy().setScientific_name(anno.getOrganism());
        }
        if (anno.getNode_name() != null)
        {
            clade.setSequence(new Sequence());
            clade.getSequence().setAccession(new Accession());
            clade.getSequence().getAccession().setValue(cutAccessionString(anno.getNode_name()));
            clade.getSequence().getAccession().setSource("UniProtKB");
        }
        if (anno.getBranch_length() != null)
        {
            clade.setBranch_length(anno.getBranch_length());
        }
        if (anno.getGene_id() != null)
        {
            clade.setName(cutGeneID(anno.getGene_id()));
        }
    }

//    https://www.geeksforgeeks.org/generic-tree-level-order-traversal/
//    Same algorithim as level order traversal for n-ary trees(using queue) except
//    instead of printing when visiting the node, we set clade node attributes equal to annotation
//    node attributes.
    static void constructPhyloTreeWithAnnoTree(Annotation root, Clade croot)
{
    if (root == null || croot == null)
        return;
    Queue<Annotation> anno_q = new LinkedList<Annotation>();
    Queue<Clade> clade_q = new LinkedList<Clade>();
    clade_q.add(croot);
    anno_q.add(root);
    while (!anno_q.isEmpty())
    {
        int n = anno_q.size();
        while (n > 0)
        {
            Annotation annoTempNode = anno_q.poll();
            Clade cladeTempNode = clade_q.poll();
            createAndSetCladeWithAno(annoTempNode, cladeTempNode);  // This replaces printing the node
            // without this, once you get to leaf node, you'll try to access thru a null object
            if (annoTempNode.getChildren() != null)
            {
                // in order to add n number of children, we need to allocate the child clade
                cladeTempNode.setClade(new LinkedList<Clade>());
                for (int i = 0; i < annoTempNode.getChildren().getAnnotation_node().size(); i++)
                {
                    cladeTempNode.getClade().add(new Clade());    // adds n childs based on the annotation nodes amount of childs
                    if (annoTempNode.getChildren().getAnnotation_node().get(i) != null)
                    {
                        anno_q.add(annoTempNode.getChildren().getAnnotation_node().get(i));
                        // add empty clade children into queue to traverse and create next children
                        clade_q.add(cladeTempNode.getClade().get(i));
                    }
                }
            }
            n--;
        }
    }
}

//    Given a module.phyloxml.Phyloxml object, creates a local xml file formatted to be easier to read. Change the number in
//    the parameter to change the amount of indent space
//    https://stackoverflow.com/questions/46708498/jaxb-marshaller-indentation fixed indent bug looking at this
//    change path to place phyloxml elsewhere
    private static void phyloObjToXML(Phyloxml phylo, String filePath)
    {
        try
        {
            //Create JAXB Context
            JAXBContext jaxbContext = JAXBContext.newInstance(Phyloxml.class);

            //Create Marshaller
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            //Store XML to File
            File file = new File(filePath);

            DOMResult domResult = new DOMResult();
            //Writes XML file to file-system
            jaxbMarshaller.marshal(phylo, domResult);

            //Required formatting
            Transformer transformer = null;
            transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            transformer.transform(new DOMSource(domResult.getNode()), new StreamResult(file));

        }
        catch (JAXBException e)
        {
            e.printStackTrace();
        } catch (TransformerException e)
        {
            e.printStackTrace();
        }
    }

//    Extracts the Q9UTA6 out of SCHPO|PomBase=SPAC25B8.12c|UniProtKB=Q9UTA6
    private static String cutAccessionString(String x)
    {
        String[] y = x.split("UniProtKB=");
        return (y[1]);
    }

    private static String cutGeneID(String id)
    {
        String[] x = id.split(":");
        return (x[1]);
    }
}