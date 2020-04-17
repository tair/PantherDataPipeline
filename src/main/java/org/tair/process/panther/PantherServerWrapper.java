package org.tair.process.panther;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;
import org.tair.module.panther.MSAList;
import org.tair.module.panther.MsaData;
import org.tair.module.panther.SearchResult;
import org.tair.util.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Class contains all calls to the Panther server
public class PantherServerWrapper {
//    private String BASE_URL = "http://panthertest3.med.usc.edu:8083/tempFamilySearch";
    private String PANTHER_FL_URL = "http://pantherdb.org/services/oai/pantherdb/supportedpantherfamilies";
    private String BASE_URL = "http://pantherdb.org/services/oai/pantherdb/treeinfo";
    private String BASE_MSA_URL = "http://pantherdb.org/services/oai/pantherdb/familymsa";
    private String URL_PTHR_FAMILY_LIST = BASE_URL+"?type=family_list";
    private String URL_PTHR_FAMILY_NAME = BASE_URL+"?type=family_name";

    //Panther 15.0 -
    private int[] taxon_filters_arr = {13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,
            70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,
            559292,284812,3708,4072,71139,51240,4236,3983,4432,88036,4113,3562};

    public int getCount_allFamilies() throws Exception {
        String url = PANTHER_FL_URL;
        String jsonText = Util.readJsonFromUrl(url);
        JSONObject jsonObj = new JSONObject(jsonText);
        int number_of_families = jsonObj.getJSONObject("search").getInt("number_of_families");
        return number_of_families;
    }

    //Get Panther Family List json from server with startIdx
    public String getPantherFamilyListFromServer(int startIdx) throws Exception {
        String url = PANTHER_FL_URL + "?startIndex="+startIdx;
        //If URL returns XML, use readContentFromWebUrlToJsonString
        return Util.readJsonFromUrl(url);
    }

    //Get Panther Family Name for given id using panther url
    public String getFamilyNameFromServer(String family_id) throws Exception {
        String flUrl = URL_PTHR_FAMILY_NAME + "&family="+family_id;
        String jsonString = Util.readFamilyNameFromUrl(flUrl);
        String familyName = new JSONObject(jsonString).getJSONObject("search").getString("family_name");
        return familyName;
    }

    //Get Panther Book Info for given id using id and taxon filters (to get pruned trees)
    public String readPantherTreeById(String family_id) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String prunedTreeUrl = BASE_URL+"?family=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        String jsonString = Util.readJsonFromUrl(prunedTreeUrl);
        return jsonString;
    }

    public String readMsaByIdFromServer(String family_id) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String msaTreeUrl = BASE_MSA_URL+"?family=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        return Util.readJsonFromUrl(msaTreeUrl);
    }

    public static void main(String args[]) throws Exception {
        PantherServerWrapper ps = new PantherServerWrapper();
        ps.readMsaByIdFromServer("PTHR10000");
    }
}
