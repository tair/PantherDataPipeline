package org.tair.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.tair.module.PantherData;
import org.tair.process.PantherBookXmlToJson;
import org.tair.process.panther.PantherETLPipeline;
import org.tair.process.panther.PantherServerWrapper;
import org.tair.util.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CrossOrigin(origins = "*")
@RestController
public class PruningController {
    PantherServerWrapper pantherServer = new PantherServerWrapper();
    private String BASE_URL = "http://pantherdb.org/tempFamilySearch";
    private String BOOK_INFO_URL = BASE_URL+"?type=book_info";
    private String GRAFT_URL = "http://pantherdb.org/services/oai/pantherdb/graftsequence";

    //Panther 15.0 -
    private int[] taxon_filters_arr = {13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,
            70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,
            559292,284812,3708,4072,71139,51240,4236,3983,4432,88036,4113,3562};

    @PostMapping(path = "/panther/pruning/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedTree(@PathVariable("id") String treeId,
                                              @RequestBody TaxonObj taxonObj) throws Exception {
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        int[] taxon_array = taxonIdsToShow.stream().mapToInt(Integer::parseInt).toArray();
        return pantherServer.readPrunedPantherTreeById(treeId, taxon_array);
    }

    @PostMapping(path = "/panther/grafting", consumes="application/json")
    public @ResponseBody String getGrafterTree(@RequestBody SequenceObj sequenceObj) throws Exception {
        String seq = sequenceObj.getSequence();
        return callGraftingApi(seq, taxon_filters_arr);
    }

    public String callGraftingApi(String seq, int[] taxon_filters_arr) {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String graftingUrl = GRAFT_URL + "?sequence=" + seq +
                "&taxonFltr=" + taxonFiltersParam;
        System.out.println("Got Grafting Request " + graftingUrl);

        String jsonString = "";
        try {
            jsonString = Util.readJsonFromUrl(graftingUrl);
        }
        catch(Exception e) {
            System.out.println("Error "+ e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

    @PostMapping(path = "/panther/grafting/prune", consumes="application/json")
    public @ResponseBody String getPrunedAndGraftedTree(@RequestBody ObjectNode json) throws Exception {
//        System.out.println(json);
        String inputSeq = json.get("sequence").asText();
        ObjectMapper mapper = new ObjectMapper();
        // acquire reader for the right type
        ObjectReader reader = mapper.readerFor(new TypeReference<List<String>>() {
        });
        List<String> taxonIds = reader.readValue(json.get("taxonIdsToShow"));
        int[] taxon_array = taxonIds.stream().mapToInt(Integer::parseInt).toArray();
        String taxonFiltersParam = IntStream.of(taxon_array)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));

        String graftingUrl = GRAFT_URL + "?sequence=" + inputSeq +
                "&taxonFltr=" + taxonFiltersParam;
        System.out.println("Got Grafting Request " + graftingUrl);

        String jsonString = "";
        try {
            jsonString = Util.readJsonFromUrl(graftingUrl);
        }
        catch(Exception e) {
            System.out.println("Error "+ e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

    public void testPruningApi() throws Exception {
        String family_id = "PTHR10683";
        String filterIds = "3702";
        String prunedTreeUrl = BOOK_INFO_URL + "&book=" + family_id + "&taxonFltr=" + filterIds;
        System.out.println(prunedTreeUrl);
        String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);
        System.out.println(jsonString);
    }

    public void testGraftingApi() throws Exception {
        String seq = "MPTFEIHDEAWYPWILGGLFALSLVTYWACDRITAPYGRHVKRGWGPAWGVRECWIVMESPALWAMVLFYSMGEQKLGRVPLILLRLHQVHYFNRVLIYPMRMKVRGKGMPIIVAACAFAFNILNSYVQARWLSNYGSYPDSWLTSPKFILGATLFGLGFLGNFWSDSYLFSLRADEDDRSYKIPKAGLFKFITCPNYFSEMVEWLGWAIMTWSPAGLAFFIYTIANLAPRAVSNHQWYLSKFNDYPKERRILIPFVY";
        String jsonStr = callGraftingApi(seq, taxon_filters_arr);
        System.out.println(jsonStr);
    }

    public void testPrunedGraftingApi() throws Exception {
        int[] taxon_filters_arr = {13333,3702};
        String seq = "MPTFEIHDEAWYPWILGGLFALSLVTYWACDRITAPYGRHVKRGWGPAWGVRECWIVMESPALWAMVLFYSMGEQKLGRVPLILLRLHQVHYFNRVLIYPMRMKVRGKGMPIIVAACAFAFNILNSYVQARWLSNYGSYPDSWLTSPKFILGATLFGLGFLGNFWSDSYLFSLRADEDDRSYKIPKAGLFKFITCPNYFSEMVEWLGWAIMTWSPAGLAFFIYTIANLAPRAVSNHQWYLSKFNDYPKERRILIPFVY";
        String jsonStr = callGraftingApi(seq, taxon_filters_arr);
        System.out.println(jsonStr);
    }

    public static void main(String args[]) throws Exception {
        PruningController controller = new PruningController();
//        controller.testPruningApi();
//        controller.testGraftingApi();
        controller.testPrunedGraftingApi();
    }

}