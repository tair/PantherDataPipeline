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
import org.tair.util.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@CrossOrigin
public class PruningController {
//    private String BASE_URL = "http://panthertest3.med.usc.edu:8083/tempFamilySearch";
    private String BASE_URL = "http://pantherdb.org/tempFamilySearch";
    private String GRAFTING_BASE_URL = "http://panthertest10.med.usc.edu:8090/tempFamilySearch";
    private String BOOK_INFO_URL = BASE_URL+"?type=book_info";
    private String GRAFT_URL = GRAFTING_BASE_URL+"?type=graft_seq";

    //Panther 15.0 -
    private int[] taxon_filters_arr = {13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,
            70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,
            559292,284812,3708,4072,71139,51240,4236,3983,4432,88036,4113,3562};

    @PostMapping(path = "/panther/pruning/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedTree(@PathVariable("id") String treeId,
                                              @RequestBody TaxonObj taxonObj) throws Exception {

        PantherETLPipeline etl = new PantherETLPipeline();
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        String family_id = treeId;
        int[] taxon_array = taxonIdsToShow.stream().mapToInt(Integer::parseInt).toArray();
        StringBuilder stringBuilder = new StringBuilder();
        String separator = ",";
        for (int i = 0; i < taxon_array.length - 1; i++) {
            stringBuilder.append(taxon_array[i]);
            stringBuilder.append(separator);
        }
        stringBuilder.append(taxon_array[taxon_array.length - 1]);
        String joined = stringBuilder.toString();

        String prunedTreeUrl = BOOK_INFO_URL + "&book=" + family_id + "&taxonFltr=" + joined;
        System.out.println(prunedTreeUrl);
        String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);

        PantherData prunedData = new PantherBookXmlToJson().convertJsonToSolrforApi(jsonString, family_id);
        return prunedData.getJsonString();
    }

    @PostMapping(path = "/panther/grafting", consumes="application/json")
    public @ResponseBody String getGrafterTree(@RequestBody SequenceObj sequenceObj) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String seq = sequenceObj.getSequence();
        String graftingUrl = GRAFT_URL + "&sequence=" + seq +
                "&taxonFltr=" + taxonFiltersParam;
        System.out.println("Got Grafting Request " + graftingUrl);

        String jsonString = "";
        try {
            jsonString = Util.readContentFromWebUrlToJsonString(graftingUrl);
        }
        catch(Exception e) {
            System.out.println("Error "+ e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

    @PostMapping(path = "/panther/grafting/prune", consumes="application/json")
    public @ResponseBody String getPrunedAndGraftedTree(@RequestBody ObjectNode json) throws Exception {

        String inputSeq = json.get("sequence").asText();
        ObjectMapper mapper = new ObjectMapper();
        // acquire reader for the right type
        ObjectReader reader = mapper.readerFor(new TypeReference<List<String>>() {
        });
        List<String> taxonIds = reader.readValue(json.get("taxonIdsToShow"));
        int[] taxon_array = taxonIds.stream().mapToInt(Integer::parseInt).toArray();


        StringBuilder stringBuilder = new StringBuilder();
        String separator = ",";
        for (int i = 0; i < taxon_array.length - 1; i++) {
            stringBuilder.append(taxon_array[i]);
            stringBuilder.append(separator);
        }
        stringBuilder.append(taxon_array[taxon_array.length - 1]);
        String joined = stringBuilder.toString();

        String graftingUrl = "http://panthertest10.med.usc.edu:8090/tempFamilySearch?type=graft_seq&sequence="+
                inputSeq + "&taxonFltr=" + joined;

        System.out.println("Got Pruned Grafting Request " + graftingUrl);
        String jsonString = "";
        try {
            jsonString = Util.readContentFromWebUrlToJsonString(graftingUrl);
        }
        catch(Exception e) {
            System.out.println("Error "+ e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

}