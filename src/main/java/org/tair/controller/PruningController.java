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

    @PostMapping(path = "/panther/pruning/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedTree(@PathVariable("id") String treeId,
                                              @RequestBody TaxonObj taxonObj) throws Exception {
        PantherETLPipeline etl = new PantherETLPipeline();
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        String family_id = treeId;
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

        return prunedData.getJsonString();
    }
}


