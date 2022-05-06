package org.tair.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin
public class StatsController {
    private String URL_SOLR = "http://54.68.67.235:8983/solr/panther";
    SolrClient solr = new HttpSolrClient.Builder(URL_SOLR).build();

    @GetMapping(path = "/panther/genecount")
    public @ResponseBody SolrDocumentList getPantherGeneCount() throws Exception {
        SolrQuery sq = new SolrQuery("*:*");
        sq.setRows(9000);
        sq.setFields("id, uniprot_ids_count");
        sq.setSort("id", SolrQuery.ORDER.asc);

        SolrRequest<QueryResponse> req = new QueryRequest(sq);
        req.setBasicAuthCredentials("phxphy", "phoenixphyl0genes");

        QueryResponse treeIdResponse = req.process(solr);
        // System.out.println(treeIdResponse.getResults());
        SolrDocumentList docList = treeIdResponse.getResults();
        return docList;
    }

    @GetMapping(path = "/panther/annotation")
    public @ResponseBody SolrDocumentList getPantherAnnotations() throws Exception {
        SolrQuery sq = new SolrQuery("*:*");
        sq.setRows(9000);
        sq.setFields("id, go_annotations_count, species_list");
        sq.setSort("id", SolrQuery.ORDER.asc);

        SolrRequest<QueryResponse> req = new QueryRequest(sq);
        req.setBasicAuthCredentials("phxphy", "phoenixphyl0genes");

        QueryResponse treeIdResponse = req.process(solr);
        // System.out.println(treeIdResponse.getResults());
        SolrDocumentList docList = treeIdResponse.getResults();
        return docList;
    }
}
