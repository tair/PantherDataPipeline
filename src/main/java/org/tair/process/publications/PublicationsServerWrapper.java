package org.tair.process.publications;

import org.json.JSONObject;
import org.tair.util.Util;

import java.util.ArrayList;
import java.util.List;

public class PublicationsServerWrapper {

    public List<String> getPublicationsByUniprotId(String uniprot_id) throws Exception {
        String url = String.format("https://www.uniprot.org/uniprot/?query=id:%s&format=tab&columns=citation,citationmapping", uniprot_id);
        System.out.println("url " + url);
        String tabbedString = Util.readContentFromWebJsonToJson(url);
        String[] lines = tabbedString.split("\n");
//        System.out.println(lines[1]);
        String[] pubmed_ids_str = lines[1].split("\t");
        List<String> pubmed_ids = new ArrayList<>();
        for(int i=0; i<pubmed_ids_str.length; i++) {
            String[] col_vals = pubmed_ids_str[i].split(";");
            for(int j=0; j<col_vals.length; j++) {
                String idx = col_vals[j].trim();
//                System.out.println(idx);
                if(!pubmed_ids.contains(idx) && !idx.isEmpty()) {
                    pubmed_ids.add(idx);
                }
            }
        }
        System.out.format("%s: Added %d\n", uniprot_id, pubmed_ids.size());
        return pubmed_ids;
    }

    public static void main(String args[]) throws  Exception{
        PublicationsServerWrapper p = new PublicationsServerWrapper();
        p.getPublicationsByUniprotId("Q9SLA2");
    }
}
