package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONObject;
import org.tair.module.Entry;
import org.tair.module.Reference;
import org.tair.module.Uniprot;
import org.tair.util.Util;

import java.io.FileNotFoundException;
import java.util.*;

public class PantherPubXmlToJson {

    private String URL_SOLR = "http://localhost:8983/solr/panther";
    SolrClient solr = new HttpSolrClient.Builder(URL_SOLR).build();

    public Collection<Object> getUniprotIdsFromTree(String pantherId) throws Exception {

        Collection <Object> uniprotIds;

        final SolrQuery question = new SolrQuery("id:"+pantherId);

        question.addField("uniprot_ids"); //object.addField("StringName" + "ObjectValue");
        final QueryResponse response = solr.query(question);
        System.out.println("response: " + response.getResults().get(0).getFieldValues("uniprot_ids"));

        uniprotIds = response.getResults().get(0).getFieldValues("uniprot_ids");

        return uniprotIds;
    }

    public List<String> getListofPapersPerUniProtID(String uniprotIds) throws Exception {

        //Initialization
        List<String> paperlist = new ArrayList<>();

        String myURL = "https://www.uniprot.org/uniprot/" + uniprotIds + ".xml";
        System.out.println(myURL);

        String intake;
        try {
            intake = Util.readContentFromWebUrlToJsonString(myURL);
        }
        catch(FileNotFoundException error){
            return paperlist;
        }
        JSONObject jsonObj = new JSONObject(intake);
        if (!jsonObj.has("uniprot")){
            return paperlist;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        Uniprot uniprotObj = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonObj.getJSONObject("uniprot").toString(), Uniprot.class);
        for (Object entryObj : uniprotObj.getEntry()){

            Entry entry = objectMapper.convertValue(entryObj, Entry.class);

            for (Object refObj : entry.getReference()){

                Reference reference = objectMapper.convertValue(refObj, Reference.class);

                for (Object pubObj : reference.getCitation()){
                    paperlist.add(objectMapper.writer().writeValueAsString(pubObj));
                }
            }
        }
        return paperlist;
    }

    //Writing Atomic Updating on Publication in SolrJ
    public void atomicUpdatePublication(String publications, String pantherId) throws Exception{

        //create input document
        SolrInputDocument sdoc = new SolrInputDocument();

        //Creating a Document
        sdoc.addField("id",pantherId);

        Map<String,Object> fieldModifier = new HashMap<>(1);

        fieldModifier.put("set",publications ); //Adding a field

        sdoc.addField("publications", fieldModifier);

        solr.add( sdoc );
        solr.commit();

    }
}
