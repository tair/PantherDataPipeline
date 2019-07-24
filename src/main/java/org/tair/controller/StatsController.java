package org.tair.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin
public class StatsController {
    private String URL_SOLR = "http://localhost:8983/solr/panther";
    SolrClient solr = new HttpSolrClient.Builder(URL_SOLR).build();

    @GetMapping(path = "/panther/genecount")
    public @ResponseBody
    SolrDocumentList getPantherGeneCount() throws Exception {
        SolrQuery sq = new SolrQuery("*:*");
        sq.setRows(9000);
        sq.setFields("id, uniprot_ids_count");
        sq.setSort("id", SolrQuery.ORDER.asc);
        QueryResponse treeIdResponse = solr.query(sq);
//        System.out.println(treeIdResponse.getResults());
        SolrDocumentList docList = treeIdResponse.getResults();
        return docList;
    }
}
