package org.tair.module.ortho;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrthoMapping {
    private OrthoSearchResult search;

    // ***Special case for Arabidopsis genes: Gene ID in Panther is either AGI ID or
    // locus ID.
    // On the download file we will show AGI ID. Mapping input gives thh mapping for
    // this conversion
    public ArrayList getAllMapped(HashMap<String, String> locus_mapping, HashMap<String, String> org_mapping)
            throws Exception {
        ArrayList listOfmapping = new ArrayList();
        if (this.getSearch().getMapping() == null) {
            System.out.println("mapping is null");
            return listOfmapping;
        }
        // if (this.getSearch().getMapping().getMapped() == null) {
        // System.out.println(this.getSearch().getMapping());
        // System.out.println("mapped is null");
        // return listOfmapping;
        // }
        for (int i = 0; i < this.getSearch().getMapping().getMappedList().size(); i++) {
            // System.out.println(this.getSearch().getMapping());
            OrthoMapped m = this.getSearch().getMapping().getMappedList().get(i);
            HashMap mMap = new HashMap();
            // https://conf.arabidopsis.org/display/PHYL/New+PantherDB+API (Sec4: Orthologs)
            String gene_id = m.getTarget_gene();
            // System.out.println(gene_id);
            String organism_code = gene_id.split("\\|")[0];
            String organism_name = organism_code;
            if (org_mapping.get(organism_code) != null) {
                organism_name = org_mapping.get(organism_code);
            }
            // System.out.println(organism_name);
            String extracted_gene_id = gene_id.split("\\|")[1];
            String code = extracted_gene_id.split("=", 2)[0];
            if (code.equals("TAIR")) {
                String val = extracted_gene_id.split("=", 2)[1];
                val = val.split("=", 2)[1];
                String updatedGeneId = locus_mapping.get(val);
                extracted_gene_id = updatedGeneId;
            } else {
                extracted_gene_id = extracted_gene_id.split("=", 2)[1];
            }
            String uniprot_id = gene_id.split("UniProtKB=")[1];
            mMap.put("gene_id", extracted_gene_id);
            mMap.put("organism", organism_name);
            mMap.put("uniprot_id", uniprot_id);
            mMap.put("ortholog", m.getOrtholog());
            listOfmapping.add(mMap);
        }
        System.out.println("listOfmapping size: " + listOfmapping.size());
        return listOfmapping;
    }

    public List<OrthoMapped> getAllMappedOrtho() {
        return this.search.mapping.getMappedList();
    }

}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class OrthoSearchResult {
    // "product": {
    // "version": 15, "content": "PANTHERDB"
    // },
    private Product product;
    public Mapping mapping;
}

@Data
class Product {
    private int version;
    private String content;
}

@Data
class Mapping {
    @JsonProperty("mapped")
    private JsonNode mapped;
    private List<OrthoMapped> mappedList;
    private OrthoMapped singleMapped;

    @JsonSetter("mapped")
    public void setMapped(JsonNode mapped) {
        if (mapped instanceof ArrayNode) {
            ObjectMapper mapper = new ObjectMapper();
            // acquire reader for the right type
            ObjectReader reader = mapper.readerFor(new TypeReference<List<OrthoMapped>>() {
            });
            try {
                this.mappedList = reader.readValue(mapped);
            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            this.mappedList = new ArrayList<>();
            // ObjectMapper mapper = new ObjectMapper();
            // // acquire reader for the right type
            // ObjectReader reader = mapper.readerFor(new TypeReference<Mapped>() {});
            // try {
            // Mapped single_mapped = reader.readValue(mapped);
            // this.mappedList = new ArrayList<>();
            // this.mappedList.add(single_mapped);
            // } catch (Exception e) {
            // System.out.println(e);
            // }
        }
    }

    public List<OrthoMapped> getMappedList() {
        return mappedList;
    }

    public OrthoMapped getSingleMapped() {
        return singleMapped;
    }
}
