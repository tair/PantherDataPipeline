package org.tair.process.panther;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;
import org.tair.module.ortho.OrthoMapped;
import org.tair.module.ortho.OrthoMapping;
import org.tair.module.paralog.Mapped;
import org.tair.module.paralog.ParalogMapping;
import org.tair.util.Util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Class contains all calls to the Panther server
public class PantherServerWrapper {
    private String PANTHER_ROOT = "http://pantherdb.org/services/oai/pantherdb/";
    private String PANTHER_FL_URL = PANTHER_ROOT + "supportedpantherfamilies";
    private String PANTHER_BASE_URL = PANTHER_ROOT + "treeinfo";
    private String BASE_MSA_URL = PANTHER_ROOT + "familymsa";
    private String PANTHER_ORTHO_URL = PANTHER_ROOT + "ortholog";

    private String RESOURCES_DIR = "src/main/resources";

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
    private int[] taxon_filters_arr = { 13333, 3702, 15368, 51351, 3055, 2711, 3659, 4155, 3847, 3635, 4232, 112509,
            3880, 214687, 4097, 39947, 105231, 3197, 3218, 3694, 3760, 3988, 4555, 4081, 4558, 3641, 4565, 29760, 4577,
            29655, 3708, 4072, 71139, 51240, 4236, 3983, 4432, 88036, 4113, 3562, 6239, 7955, 44689, 7227, 83333, 9606,
            10090, 10116, 559292, 284812 };

    public PantherServerWrapper() {
        loadProps();
    }

    private void loadProps() {
        try {
            InputStream input = new FileInputStream(RESOURCES_DIR + "/application.properties");
            // load props
            Properties prop = new Properties();
            prop.load(input);
            System.out.println(prop);
            if (prop.containsKey("PANTHER_FL_URL")) {
                PANTHER_FL_URL = prop.getProperty("PANTHER_FL_URL");
            }
            if (prop.containsKey("PANTHER_BASE_URL")) {
                PANTHER_BASE_URL = prop.getProperty("PANTHER_BASE_URL");
            }
            if (prop.containsKey("BASE_MSA_URL")) {
                BASE_MSA_URL = prop.getProperty("BASE_MSA_URL");
            }

        } catch (Exception e) {
            // System.out.println("Prop file not found!");
        }
    }

    public int getCount_allFamilies() throws Exception {
        String url = PANTHER_FL_URL;
        String jsonText = Util.readJsonFromUrl(url);
        JSONObject jsonObj = new JSONObject(jsonText);
        int number_of_families = jsonObj.getJSONObject("search").getInt("number_of_families");
        return number_of_families;
    }

    // Get Panther Family List json from server with startIdx
    public String getPantherFamilyListFromServer(int startIdx) throws Exception {
        String url = PANTHER_FL_URL + "?startIndex=" + startIdx;
        // If URL returns XML, use readContentFromWebUrlToJsonString
        return Util.readJsonFromUrl(url);
    }

    // Get Panther Book Info for given id using id and taxon filters (to get pruned
    // trees)
    public String readPantherTreeById(String family_id) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String prunedTreeUrl = PANTHER_BASE_URL + "?family=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        String jsonString = Util.readJsonFromUrl(prunedTreeUrl);
        return jsonString;
    }

    // Get Panther Tree dynamically using selected taxon ids
    public String readPrunedPantherTreeById(String family_id, int[] selected_taxons) throws Exception {
        String taxonFiltersParam = IntStream.of(selected_taxons)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String prunedTreeUrl = PANTHER_BASE_URL + "?family=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        System.out.println(prunedTreeUrl);
        String jsonString = Util.readJsonFromUrl(prunedTreeUrl);
        return jsonString;
    }

    public String readMsaByIdFromServer(String family_id) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String msaTreeUrl = BASE_MSA_URL + "?family=" + family_id + "&taxonFltr=" + taxonFiltersParam;
        return Util.readJsonFromUrl(msaTreeUrl);
    }

    public String callOrtholog_uniprot(String uniprotId, Map<String, String> uniprot2agi_mapping) throws Exception {
        String taxonFiltersParam = IntStream.of(taxon_filters_arr)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining("%2C"));
        String url = PANTHER_ORTHO_URL + "/matchortho?geneInputList=" + uniprotId
                + "&organism=3702&targetOrganism=" +
                taxonFiltersParam
                + "&orthologType=all";
        String jsonString = Util.readJsonFromUrl(url);
        // System.out.println(jsonString);
        OrthoMapping orthoResult = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
                OrthoMapping.class);
        List<OrthoMapped> orthologs = orthoResult.getAllMappedOrtho();
        for (int i = 0; i < orthologs.size(); i++) {
            // System.out.println(orthologs.get(i).getTarget_gene_symbol());
            OrthoMapped mapped = orthologs.get(i);
            String target_gene = mapped.getTarget_gene();
            String uniprot_id = target_gene.split("UniProtKB=")[1];
            mapped.setUniprot_id(uniprot_id);
            String organism = target_gene.split("\\|")[0];
            // System.out.println(organism);
            mapped.setOrganism(organism);
            String gene_id = target_gene.split("\\|")[1];
            // System.out.println(gene_id);
            mapped.setTarget_gene_id(gene_id);
        }
        ObjectMapper mapper = new ObjectMapper();
        String processedOrthologsJsonStr = mapper.writeValueAsString(orthologs);
        return processedOrthologsJsonStr;
    }

    public String callParalog_uniprot(String uniprotId, Map<String, String> uniprot2agi_mapping) throws Exception {
        String url = PANTHER_ORTHO_URL + "/homologOther?geneInputList=" + uniprotId + "&organism=3702&homologType=P";
        String jsonString = Util.readJsonFromUrl(url);
        try {
            ParalogMapping paraResult = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(
                    jsonString,
                    ParalogMapping.class);
            List<Mapped> paralogs = paraResult.getAllMappedParalogs();
            for (int i = 0; i < paralogs.size(); i++) {
                Mapped para_obj = paralogs.get(i);
                String target_gene = para_obj.getTarget_gene();
                String uniprot_id = target_gene.split("UniProtKB=")[1];
                String agi_id = uniprot2agi_mapping.get(uniprot_id);
                // System.out.println(uniprot_id + "->" + agi_id);
                para_obj.setTarget_uniprot(uniprot_id);
                para_obj.setTarget_agi(agi_id);
            }

            ObjectMapper mapper = new ObjectMapper();
            String processedParalogsJsonStr = mapper.writeValueAsString(paralogs);
            // System.out.println(processedParalogsJsonStr);
            return processedParalogsJsonStr;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public static void main(String args[]) throws Exception {
        PantherServerWrapper ps = new PantherServerWrapper();
        int[] taxonIds = { 3702 };
        // ps.readMsaByIdFromServer("PTHR10000");
        ps.readPrunedPantherTreeById("PTHR11913", taxonIds);

    }
}
