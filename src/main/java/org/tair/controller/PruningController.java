package org.tair.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.tair.module.PantherData;
import org.tair.module.ortho.OrthoMapping;
import org.tair.process.panther.PantherETLPipeline;
import org.tair.process.panther.PantherLocalWrapper;
import org.tair.process.panther.PantherServerWrapper;
import org.tair.process.panther.PhylogenesServerWrapper;
import org.tair.util.Util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CrossOrigin(origins = "*")
@RestController
public class PruningController {
    @Autowired
    PhylogenesServerWrapper pgServer;
    PantherServerWrapper pantherServer = new PantherServerWrapper();
    PantherLocalWrapper pantherLocal = new PantherLocalWrapper();
    private String BASE_URL = "https://pantherdb.org";
    // private String BASE_URL = "http://panthertest9.med.usc.edu:8089";
    private String BOOK_INFO_URL = BASE_URL + "?type=book_info";
    private String GRAFT_URL = BASE_URL + "/services/oai/pantherdb/graftsequence";
    private String ORTHO_URL = BASE_URL + "/services/oai/pantherdb/ortholog/matchortho?geneInputList=";

    // Panther 15.0 -
    // private int[] taxon_filters_arr = { 13333, 3702, 15368, 51351, 3055, 2711,
    // 3659, 4155, 3847, 3635, 4232, 112509,
    // 3880, 214687, 4097, 39947,
    // 70448, 42345, 3218, 3694, 3760, 3988, 4555, 4081, 4558, 3641, 4565, 29760,
    // 4577, 29655, 6239, 7955, 44689,
    // 7227, 83333, 9606, 10090, 10116,
    // 559292, 284812, 3708, 4072, 71139, 51240, 4236, 3983, 4432, 88036, 4113, 3562
    // };

    // Panther 17.0 -
    private int[] taxon_filters_arr = {
            13333, 3702, 15368, 51351, 3055, 2711, 3659, 4155, 3847, 3635, 4232, 112509, 3880, 214687, 4097, 39947,
            105231, 3197, 3218, 3694, 3760, 3988, 4555, 4081, 4558, 3641, 4565, 29760, 4577, 29655, 3708, 4072, 71139,
            51240, 4236, 3983, 4432, 88036, 4113, 3562, 6239, 7955, 44689, 7227, 83333, 9606, 10090, 10116, 559292,
            284812
    };

    private int[] organism_taxon_ids = { 3702, 13333, 15368, 51351, 3055, 2711, 3659, 4155, 3847, 3635, 4232, 112509,
            3880, 214687, 4097, 39947,
            70448, 42345, 3218, 3694, 3760, 3988, 4555, 4081, 4558, 3641, 4565, 29760, 4577, 29655, 3708, 4072, 71139,
            51240, 4236, 3983, 4432, 88036, 4113, 3562 };

    PantherETLPipeline etl = new PantherETLPipeline();
    HashMap<String, String> tair_locus2id_mapping;
    HashMap<String, String> org_mapping;

    Logger logger = Logger.getLogger(PruningController.class.getName());

    PruningController() throws Exception {
        tair_locus2id_mapping = pantherLocal.read_locus2tair_mapping_csv();
        org_mapping = pantherLocal.read_org_mapping_csv();
    }

    @PostMapping(path = "/panther/pruning/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedTree(@PathVariable("id") String treeId,
            @RequestBody TaxonObj taxonObj) throws Exception {
        logger.log(Level.INFO, () -> String.format("getPrunedTree: treeId {}", treeId));
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        int[] taxon_array = taxonIdsToShow.stream().mapToInt(Integer::parseInt).toArray();
        String prunedTree = pantherServer.readPrunedPantherTreeById(treeId, taxon_array);
        String processedTree = etl.processPrunedTree(prunedTree);
        return processedTree;
    }

    @PostMapping(path = "/panther/grafting", consumes = "application/json")
    public @ResponseBody String getGrafterTree(@RequestBody SequenceObj sequenceObj) throws Exception {
        logger.log(Level.INFO, () -> String.format("getGrafterTree: sequenceObj {}", sequenceObj.getSequence()));
        String seq = sequenceObj.getSequence();
        return callGraftingApi(seq, taxon_filters_arr);
    }

    @PostMapping(path = "/panther/grafting/prune", consumes = "application/json")
    public @ResponseBody String getPrunedAndGraftedTree(@RequestBody ObjectNode json) throws Exception {
        logger.log(Level.INFO, () -> String.format("getGrafterTree: sequence {}", json.get("sequence").asText()));
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
        } catch (Exception e) {
            System.out.println("Error " + e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

    @PostMapping(path = "/panther/orthomapping", consumes = "application/json")
    public @ResponseBody String getOrthoMapping(@RequestBody OrthoObj orthoObj) throws Exception {
        logger.log(Level.INFO, () -> String.format("getOrthoMapping: uniprotId {}", orthoObj.getUniprotId()));
        if (orthoObj.getQueryOrganismId() == null) {
            return "";
        }
        int queryId = Integer.parseInt(orthoObj.getQueryOrganismId());
        return callOrthologApi(orthoObj.getUniprotId(), queryId);
    }

    @PostMapping(path = "/panther/fastadoc/{id}", consumes = "application/json")
    public @ResponseBody String getFastaDoc(@PathVariable("id") String treeId) throws Exception {
        logger.log(Level.INFO, () -> String.format("getFastaDoc: treeId {}", treeId));
        return callFastaApi(treeId, null);
    }

    @PostMapping(path = "/panther/pruning/fastadoc/{id}", consumes = "application/json")
    public @ResponseBody String getPrunedFastaDoc(@PathVariable("id") String treeId,
            @RequestBody TaxonObj taxonObj) throws Exception {
        logger.log(Level.INFO, () -> String.format("getPrunedFastaDoc: treeId {}", treeId));
        List<String> taxonIdsToShow = taxonObj.getTaxonIdsToShow();
        int[] taxon_array = taxonIdsToShow.stream().mapToInt(Integer::parseInt).toArray();
        return callFastaApi(treeId, taxon_array);
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
        } catch (Exception e) {
            System.out.println("Error " + e.getMessage());
            return e.getMessage();
        }
        return jsonString;
    }

    public String callOrthologApi(String uniprotId, int queryOrganismId) throws IOException {
        List<Integer> list = Arrays.stream(organism_taxon_ids).boxed().collect(Collectors.toList());
        list.remove(Integer.valueOf(queryOrganismId));
        int[] arr = list.stream().mapToInt(i -> i).toArray();
        String taxonFiltersParam = IntStream.of(arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining("%2C"));
        String orthologUrl = ORTHO_URL + uniprotId + "&organism=" + queryOrganismId + "&targetOrganism="
                + taxonFiltersParam
                + "&orthologType=all";
        System.out.println("orthologUrl " + orthologUrl);
        JSONObject orthoTree;
        try {
            orthoTree = Util.getJsonObjectFromUrl(orthologUrl);
            OrthoMapping orthoMapping = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(
                    orthoTree.toString(),
                    OrthoMapping.class);
            // System.out.println(orthoMapping.toString());
            JSONArray json = new JSONArray(orthoMapping.getAllMapped(tair_locus2id_mapping, org_mapping));
            return json.toString();
        } catch (Exception e) {
            System.out.println("callOrthologApi: Error " + e.toString());
            return "{}";
        }
    }

    public String callFastaApi(String treeId, int[] taxon_array) throws Exception {
        String fasta_doc = "";
        if (taxon_array == null || taxon_array.length == 0) {
            fasta_doc = pgServer.getFastaDocFromTree(treeId);
        } else {
            fasta_doc = pgServer.getFastaDocForPrunedTree(treeId, taxon_array);
        }
        return fasta_doc;
    }

    public void testPruningApi() throws Exception {
        String family_id = "PTHR10683";
        String filterIds = "3702";
        String prunedTreeUrl = BOOK_INFO_URL + "&book=" + family_id + "&taxonFltr=" + filterIds;
        // System.out.println(prunedTreeUrl);
        String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);
        // System.out.println(jsonString);
    }

    public void testPruningProcess() throws Exception {
        int[] taxon_array = { 3702 };
        String treeId = "PTHR11913";
        String prunedTree = pantherServer.readPrunedPantherTreeById(treeId, taxon_array);
        etl.processPrunedTree(prunedTree);
    }

    public void testGraftingApi() throws Exception {
        String seq = "MPTFEIHDEAWYPWILGGLFALSLVTYWACDRITAPYGRHVKRGWGPAWGVRECWIVMESPALWAMVLFYSMGEQKLGRVPLILLRLHQVHYFNRVLIYPMRMKVRGKGMPIIVAACAFAFNILNSYVQARWLSNYGSYPDSWLTSPKFILGATLFGLGFLGNFWSDSYLFSLRADEDDRSYKIPKAGLFKFITCPNYFSEMVEWLGWAIMTWSPAGLAFFIYTIANLAPRAVSNHQWYLSKFNDYPKERRILIPFVY";
        String jsonStr = callGraftingApi(seq, taxon_filters_arr);
        // System.out.println(jsonStr);
    }

    public void testPrunedGraftingApi() throws Exception {
        int[] taxon_filters_arr = { 13333, 3702 };
        String seq = "MPTFEIHDEAWYPWILGGLFALSLVTYWACDRITAPYGRHVKRGWGPAWGVRECWIVMESPALWAMVLFYSMGEQKLGRVPLILLRLHQVHYFNRVLIYPMRMKVRGKGMPIIVAACAFAFNILNSYVQARWLSNYGSYPDSWLTSPKFILGATLFGLGFLGNFWSDSYLFSLRADEDDRSYKIPKAGLFKFITCPNYFSEMVEWLGWAIMTWSPAGLAFFIYTIANLAPRAVSNHQWYLSKFNDYPKERRILIPFVY";
        String jsonStr = callGraftingApi(seq, taxon_filters_arr);
        // System.out.println(jsonStr);
    }

    public void testOrtholog() throws Exception {
        String jsonStr = callOrthologApi("A0A1S3YGD9", 4097);
    }

    public void testFasta() throws Exception {
        String jsonStr = callFastaApi("PTHR22166", null);
    }

    public static void main(String args[]) throws Exception {
        PruningController controller = new PruningController();
        controller.testFasta();
        // controller.testOrtholog();
        // controller.testPruningApi();
        // controller.testGraftingApi();
        // controller.testPrunedGraftingApi();
        // controller.testPruningProcess();
    }

}