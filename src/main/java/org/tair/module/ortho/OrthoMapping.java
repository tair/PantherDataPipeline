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
        if (this.getSearch() == null || this.getSearch().getMapping() == null) {
            System.out.println("search or mapping is null");
            return listOfmapping;
        }

        List<OrthoMapped> mappedList = this.getSearch().getMapping().getMappedList();
        if (mappedList == null || mappedList.isEmpty()) {
            System.out.println("No mapped results found");
            return listOfmapping;
        }

        for (OrthoMapped m : mappedList) {
            HashMap mMap = new HashMap();
            String gene_id = m.getTarget_gene();
            if (gene_id == null) {
                continue;
            }

            String[] parts = gene_id.split("\\|");
            if (parts.length < 2) {
                continue;
            }

            String organism_code = parts[0];
            String organism_name = organism_code;
            if (org_mapping != null && org_mapping.get(organism_code) != null) {
                organism_name = org_mapping.get(organism_code);
            }

            String extracted_gene_id = parts[1];
            String[] gene_parts = extracted_gene_id.split("=", 2);
            if (gene_parts.length < 2) {
                continue;
            }

            String code = gene_parts[0];
            if (code.equals("TAIR") && locus_mapping != null) {
                String val = gene_parts[1];
                if (val.contains("=")) {
                    val = val.split("=", 2)[1];
                }
                String updatedGeneId = locus_mapping.get(val);
                if (updatedGeneId != null) {
                    extracted_gene_id = updatedGeneId;
                }
            } else {
                extracted_gene_id = gene_parts[1];
            }

            String uniprot_id = "";
            if (gene_id.contains("UniProtKB=")) {
                uniprot_id = gene_id.split("UniProtKB=")[1];
            }

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
        return this.search != null && this.search.mapping != null ? 
               this.search.mapping.getMappedList() : new ArrayList<>();
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
    private List<OrthoMapped> mappedList = new ArrayList<>();
    private OrthoMapped singleMapped;
    @JsonProperty("unmapped_ids")
    private JsonNode unmappedIds;

    @JsonSetter("mapped")
    public void setMapped(JsonNode mapped) {
        if (mapped != null && mapped instanceof ArrayNode) {
            ObjectMapper mapper = new ObjectMapper();
            // acquire reader for the right type
            ObjectReader reader = mapper.readerFor(new TypeReference<List<OrthoMapped>>() {
            });
            try {
                this.mappedList = reader.readValue(mapped);
            } catch (Exception e) {
                System.out.println("Error parsing mapped data: " + e.getMessage());
                this.mappedList = new ArrayList<>();
            }
        } else {
            this.mappedList = new ArrayList<>();
        }
    }

    public List<OrthoMapped> getMappedList() {
        return mappedList != null ? mappedList : new ArrayList<>();
    }

    public OrthoMapped getSingleMapped() {
        return singleMapped;
    }
}
