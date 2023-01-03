package org.tair.process.panther;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import javafx.util.Pair;
import org.json.JSONObject;
import org.tair.module.ortho.OrthoMapped;
import org.tair.module.ortho.OrthoMapping;
import org.tair.module.paralog.Mapped;
import org.tair.module.paralog.ParalogMapping;
import org.tair.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
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

    private int[] taxon_filters_arr_ortho = { 13333, 15368, 51351, 3055, 2711, 3659, 4155, 3847, 3635, 4232, 112509,
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
            // System.out.println(prop);
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
            System.out.println("PantherServerWrapper: Prop file not found!");
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
        System.out.println("prunedTreeUrl " + prunedTreeUrl);
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

    public List<String> callOrtholog_uniprot(String uniprotId, Map<String, String> uniprot2agi_mapping,
            Map<String, List<String>> organisms_mapping) throws Exception {
        String taxonFiltersParam = IntStream.of(
                taxon_filters_arr_ortho)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String url = PANTHER_ORTHO_URL + "/matchortho?geneInputList=" + uniprotId
                + "&organism=3702&targetOrganism=" +
                taxonFiltersParam
                + "&orthologType=all";
        System.out.println(url);
        String jsonString = Util.readJsonFromUrl(url);
        // System.out.println(jsonString);
        OrthoMapping orthoResult = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
                OrthoMapping.class);
        List<OrthoMapped> orthologs = orthoResult.getAllMappedOrtho();
        for (int i = 0; i < orthologs.size(); i++) {
            // System.out.println(orthologs.get(i).getTarget_gene_symbol());
            OrthoMapped mapped = orthologs.get(i);
            String target_gene = mapped.getTarget_gene();
            // System.out.println(target_gene);
            if (!target_gene.contains("UniProtKB=")) {
                mapped.setUniprot_id("error");
            } else {
                String uniprot_id = target_gene.split("UniProtKB=")[1];
                mapped.setUniprot_id(uniprot_id);
            }
            String organism = target_gene.split("\\|")[0];
            // System.out.println(organism);
            mapped.setOrganism(organism);
            String gene_id = target_gene.split("\\|")[1].split("=")[1];
            // System.out.println(gene_id);
            mapped.setTarget_gene_id(gene_id);
            List<String> organism_names = organisms_mapping.get(organism);
            if (organism_names == null) {
                // System.out.println("No Mapping found for " + organism);
                mapped.setFull_name("Unknown");
                mapped.setCommon_name("");
                mapped.setGroup_name("Unknown");
            } else {
                String full_name = organism_names.get(0);
                String common_name = organism_names.get(1);
                String group_name = organism_names.get(2);

                mapped.setFull_name(full_name);
                mapped.setCommon_name(common_name);
                mapped.setGroup_name(group_name);
            }
        }
        orthologs.sort(new OrthoComparator());
        // System.out.println(orthologs.toString());
        StringBuilder orthoTxtSb = new StringBuilder(
                String.join("\t", "Organism", "UniProt ID", "Gene ID", "Ortholog Type\n"));
        for (int i = 0; i < orthologs.size(); i++) {
            OrthoMapped mapped = orthologs.get(i);
            // System.out.println("getUniprot_id " + mapped.getUniprot_id());
            String full_organism = mapped.getFull_name();
            String common_name = mapped.getCommon_name();
            if (common_name != null && !common_name.equals("")) {
                full_organism += " (" + common_name + ")";
            }
            orthoTxtSb.append(String.join("\t", full_organism, mapped.getUniprot_id(), mapped.getTarget_gene_id(),
                    mapped.getOrtholog() + "\n"));
        }
        ObjectMapper mapper = new ObjectMapper();
        String processedOrthologsJsonStr = mapper.writeValueAsString(orthologs);
        // System.out.println("processedOrthologsJsonStr"+processedOrthologsJsonStr);
        // System.out.println("orthoTxtSb.toString() "+orthoTxtSb.toString());
        return Arrays.asList(processedOrthologsJsonStr, orthoTxtSb.toString());
    }

    public class OrthoComparator implements Comparator<OrthoMapped> {
        @Override
        public int compare(OrthoMapped o1, OrthoMapped o2) {
            // String organism1 = o1.getOrganism();
            // String organism2 = o2.getOrganism();
            if (o1.getFull_name() == null)
                return 0;
            if (o1.getFull_name().compareToIgnoreCase(o2.getFull_name()) != 0) {
                return o1.getFull_name().compareToIgnoreCase(o2.getFull_name());
            } else {
                return o1.getTarget_gene_id().compareToIgnoreCase(o2.getTarget_gene_id());
            }
        }
    }

    public List<String> callParalog_uniprot(String uniprotId, Map<String, String> uniprot2agi_mapping,
            Map<String, String> agi2symbol_mapping) throws Exception {
        String url = PANTHER_ORTHO_URL + "/homologOther?geneInputList=" + uniprotId + "&organism=3702&homologType=P";
        String jsonString = Util.readJsonFromUrl(url);
        int agiNullCount = 0;
        int primarySymbolNullCount = 0;
        // System.out.println("from paralog url: " + jsonString);
        try {
            ParalogMapping paraResult = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(
                    jsonString,
                    ParalogMapping.class);
            List<Mapped> paralogs = paraResult.getAllMappedParalogs();
            List<Mapped> toAdd = new ArrayList<>();
            List<Mapped> toDelete = new ArrayList<>();
            List<String> agiNullUniprotIds = new ArrayList<>();
            for (int i = 0; i < paralogs.size(); i++) {
                Mapped para_obj = paralogs.get(i);
                String target_gene = para_obj.getTarget_gene();
                if (!target_gene.contains("UniProtKB=")) {
                    para_obj.setTarget_uniprot("error");
                    para_obj.setTarget_agi("error");
                    para_obj.setPrimary_symbol("error");
                    continue;
                }
                String uniprot_id = target_gene.split("UniProtKB=")[1];
                para_obj.setTarget_uniprot(uniprot_id);
                String agi_id = uniprot2agi_mapping.get(uniprot_id);
                if (agi_id != null && agi_id.contains("/")) {
                    String[] agi_id_list = agi_id.split("/");
                    System.out.println("multiple agi_ids: ");
                    for (String agi_id_item : agi_id_list) {
                        System.out.println(agi_id_item);
                        ObjectMapper objectMapper = new ObjectMapper();
                        Mapped para_obj_temp = objectMapper.readValue(objectMapper.writeValueAsString(para_obj),
                                Mapped.class);
                        para_obj_temp.setTarget_agi(agi_id_item);
                        String primarySymbol = agi2symbol_mapping.get(agi_id_item);
                        para_obj_temp.setPrimary_symbol(primarySymbol);
                        System.out.println(primarySymbol);
                        toAdd.add(para_obj_temp);
                        toDelete.add(para_obj);
                    }
                } else if (agi_id != null) {
                    para_obj.setTarget_agi(agi_id);
                    String primarySymbol = agi2symbol_mapping.get(agi_id);
                    para_obj.setPrimary_symbol(primarySymbol);
                } else {
                    agiNullUniprotIds.add(uniprot_id);
                    para_obj.setTarget_agi(null);
                    para_obj.setPrimary_symbol(null);
                }
                // System.out.println(uniprot_id + "->" + agi_id);
                // System.out.println(agi_id + " " + primarySymbol);
            }
            paralogs.removeAll(toDelete);
            paralogs.addAll(toAdd);
            paralogs.sort(new ParaComparator());
            StringBuilder paraTxtSb = new StringBuilder(String.join("\t", "AGI ID", "Locus primary symbol\n"));
            for (int i = 0; i < paralogs.size(); i++) {
                Mapped para_obj = paralogs.get(i);
                if (para_obj.getTarget_agi() == null) {
                    agiNullCount += 1;
                } else {
                    String upperTarget_agi = para_obj.getTarget_agi().toUpperCase(Locale.ROOT);
                    para_obj.setTarget_agi(upperTarget_agi);
                }
                if (para_obj.getPrimary_symbol() == null) {
                    primarySymbolNullCount += 1;
                }
                paraTxtSb.append(String.join("\t", para_obj.getTarget_agi(), para_obj.getPrimary_symbol() + "\n"));

            }

            ObjectMapper mapper = new ObjectMapper();
            String processedParalogsJsonStr = mapper.writeValueAsString(paralogs);
            return Arrays.asList(
                    processedParalogsJsonStr,
                    paraTxtSb.toString(),
                    Integer.toString(agiNullCount),
                    Integer.toString(primarySymbolNullCount),
                    Integer.toString(paralogs.size()),
                    String.join(",", agiNullUniprotIds));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join(" ", "error downloading paralogs:", uniprotId,
                    uniprot2agi_mapping.get(uniprotId), jsonString));
            return null;
        }
    }

    public class ParaComparator implements Comparator<Mapped> {
        @Override
        public int compare(Mapped m1, Mapped m2) {
            String target_agi1 = m1.getTarget_agi() != null ? m1.getTarget_agi() : "null";
            String target_agi2 = m2.getTarget_agi() != null ? m2.getTarget_agi() : "null";
            return target_agi1.compareToIgnoreCase(target_agi2);
        }
    }

    public static void main(String args[]) throws Exception {
        PantherServerWrapper ps = new PantherServerWrapper();
        int[] taxonIds = { 3702 };
        // ps.readMsaByIdFromServer("PTHR10000");
        ps.readPrunedPantherTreeById("PTHR11913", taxonIds);
    }
}
