package org.tair.process;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.tair.module.pantherForPhylo.*;
import org.tair.module.phyloxml.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.util.Queue;
import java.util.LinkedList;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.HashMap;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.StreamResult;


/*
    Purpose:
        Input: place Panther Json Files in main/resources/panther/pruned_panther_files
        Output: receive Json files in xml format
 */
public class pantherToPhyloXmlPipeline
{
    public static void main(String args[])
    {
//        PantherJsonToPhyloXml("PTHR11705");

//          converts all panther json files in pruned_panther_files direct
//          there are some sample json files in there currently
          convertAllInDirectory();
    }

    // specifically converts all pruned pantherForPhylo files from resource directory
    // and places them into phyloxml directory
    static void convertAllInDirectory()
    {
        File dir = new File("src/main/resources/panther/pruned_panther_files");
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null)
        {
            for (File child : directoryListing)
            {
                if (child.getName().charAt(0) != '.') // to ignore files such as .gitignore and .ds_store
                    PantherJsonToPhyloXml(child.getName().replace(".json", ""));
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
        Panther panther = new Panther();
        try
        {
            // mapping pantherForPhylo json file to pantherj java object
            panther = objectMapper.readValue(new File("src/main/resources/panther/pruned_panther_files/"+fileName+".json"), Panther.class);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        // some objects in the pruned files are empty with just an ID, this avoids creating an xml for it
        if (panther.getSearch() != null)
        {
            // creating the root node and setting up transformation(pantherForPhylo -> phyloxml)
            Phyloxml phylo = createPhyloRoot(loadHashWithFamNames(), panther);
            // transformation pantherForPhylo object to phyloxml object, build phyloxml tree while level order traversal
            constructPhyloTreeWithAnnoTree(panther.getSearch().getAnnotation_node(), phylo.getPhylogeny().getClade());
            // transform phlyo java object to xml local file
            phyloObjToXML(phylo, fileName);
        }
    }

    // change path if family names placed elsewhere
    static HashMap<String, String> loadHashWithFamNames()
    {
        // declarations for loading in hashmap values for family name and id key value pairs
        ObjectMapper mapper = new ObjectMapper();
        familyList fam = new familyList();
        fam.setFamilyNames(new ArrayList<FamilyNames>());
        HashMap<String, String> hm = new HashMap<String, String>();
        try
        {
            // loading in hashmap values for family name and id key value pairs
            InputStream input = new FileInputStream("src/main/resources/panther/familyNamesList.json");
            String data = mapper.readValue(input, String.class);
            fam = mapper.readValue(data, familyList.class); //.enable(SerializationFeature.INDENT_OUTPUT)
            for (int i = 0; i < fam.getFamilyNames().size(); i++)
                hm.put(fam.getFamilyNames().get(i).getPantherId(), fam.getFamilyNames().get(i).getFamilyName());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return hm;
    }

//    Creates the first node, and classes containing the node before
//    passing that root to a function that creates the tree.
    static Phyloxml createPhyloRoot(HashMap<String, String> hm, Panther panther)
{
    Phyloxml phylo = new Phyloxml();
    phylo.setPhylogeny(new Phylogeny());
    phylo.getPhylogeny().setClade(new Clade());
    phylo.getPhylogeny().getClade().setEvents(new Events());
    phylo.getPhylogeny().setDescription(panther.getId());
    phylo.getPhylogeny().setRooted("true");                     // CHECK WHEN TO KNOW TRUE AND FALSE
    phylo.getPhylogeny().setName(hm.get(panther.getId()));
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
    private static void phyloObjToXML(Phyloxml phylo, String fileName)
    {
        try
        {
            //Create JAXB Context
            JAXBContext jaxbContext = JAXBContext.newInstance(Phyloxml.class);

            //Create Marshaller
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            //Store XML to File
            File file = new File("src/main/resources/phyloxml/"+fileName+".xml");

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