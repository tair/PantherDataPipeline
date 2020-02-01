package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;
import org.tair.module.PantherData;
import org.tair.util.Util;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Class contains all calls to the Panther server
public class PantherServerWrapper {
    private String BASE_URL = "http://panthertest3.med.usc.edu:8083/tempFamilySearch";
    private String URL_PTHR_FAMILY_LIST = BASE_URL+"?type=family_list";
    private String URL_PTHR_FAMILY_NAME = BASE_URL+"?type=family_name";

    //Panther 15.0 -
    private int[] taxon_filters_arr = {13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,
            70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,
            559292,284812,3708,4072,71139,51240,4236,3983,4432,88036,4113,3562};

    //Get Panther Family List from server and convert to a json string
    public String getPantherFamilyListFromServer() throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String url = URL_PTHR_FAMILY_LIST + "&taxonFltr=" + taxonFiltersParam;
        System.out.println(url);
        return Util.readContentFromWebUrlToJsonString(url);
    }

    //Get Panther Family Name for given id using panther url
    public String getFamilyNameFromServer(String family_id) throws Exception {
        String flUrl = URL_PTHR_FAMILY_NAME + "&book="+family_id;
        String jsonString = Util.readFamilyNameFromUrl(flUrl);
        String familyName = new JSONObject(jsonString).getJSONObject("search").getString("family_name");
        return familyName;
    }

    public PantherData readPantherTreeById(String family_id) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String prunedTreeUrl = BASE_URL+"?type=book_info&book=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);
        // convert json string to Panther object
        PantherData pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
                PantherData.class);
        pantherData.setId(family_id);
        pantherData.setJsonString(jsonString);
        return pantherData;
    }
}
