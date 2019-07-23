package org.tair.controller;

import org.springframework.web.bind.annotation.*;
import org.tair.module.PantherData;
import org.tair.process.PantherBookXmlToJson;
import org.tair.process.PantherETLPipeline;
import org.tair.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin
public class PruningController {

    @PostMapping(path = "/hello/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedTree(@PathVariable("id") String treeId,
                                              @RequestBody TaxonObj taxonObj) throws Exception {
        PantherETLPipeline etl = new PantherETLPipeline();
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        String family_id = treeId;
//        String family_name = etl.getPantherFamilyName(family_id);
//        System.out.println(family_name);
//        int[] taxon_array = {13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,
//                             39947,70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,
//                             7955,44689,7227,83333,9606,10090,10116,559292,284812};
//        int[] taxon_array = {};
        int[] taxon_array = taxonIdsToShow.stream().mapToInt(Integer::parseInt).toArray();
        System.out.println(taxon_array);
        StringBuilder stringBuilder = new StringBuilder();
        String separator = ",";
        for (int i = 0; i < taxon_array.length - 1; i++) {
            stringBuilder.append(taxon_array[i]);
            stringBuilder.append(separator);
        }
        stringBuilder.append(taxon_array[taxon_array.length - 1]);
        String joined = stringBuilder.toString();

        String prunedTreeUrl = "http://pantherdb.org/tempFamilySearch?type=book_info&book=" + family_id
                + "&taxonFltr=" + joined;
        String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);


        PantherData prunedData = new PantherBookXmlToJson().convertJsonToSolrforApi(jsonString, family_id);

//        System.out.println(prunedData);
        return prunedData.getJsonString();
    }
}


