package org.tair.module.ortho;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrthoMapping {
    private OrthoSearchResult search;
    //***Special case for Arabidopsis genes: Gene ID in Panther is either AGI ID or locus ID.
    // On the download file we will show AGI ID. Mapping input gives thh mapping for this conversion
    public ArrayList getAllMapped(HashMap<String, String> locus_mapping, HashMap<String, String> org_mapping) {
        ArrayList listOfmapping = new ArrayList();

        for (int i = 0; i < this.getSearch().getMapping().getMapped().size(); i++) {
            Mapped m = this.getSearch().getMapping().getMapped().get(i);
            HashMap mMap = new HashMap();
            //https://conf.arabidopsis.org/display/PHYL/New+PantherDB+API (Sec4: Orthologs)
            String gene_id = m.getTarget_gene();
            String organism_code = gene_id.split("\\|")[0];
            String organism_name = organism_code;
            if(org_mapping.get(organism_code) != null) {
                organism_name = org_mapping.get(organism_code);
            }
//            System.out.println(organism_name);
            String extracted_gene_id = gene_id.split("\\|")[1];
            String code = extracted_gene_id.split("=", 2)[0];
            if(code.equals("TAIR")) {
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
        return listOfmapping;
    }
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class OrthoSearchResult {
    //    "product": {
    //        "version": 15, "content": "PANTHERDB"
    //    },
    private Product product;
    private  Mapping mapping;
}

@Data
class Product {
    private int version;
    private String content;
}
@Data
class Mapping {
    private List<Mapped> mapped;
}

@Data
class Mapped {
    private String target_gene_symbol;
    private String persistent_id;
    private String target_persistent_id;
    private String ortholog;
    private String gene;
    private String target_gene;
    private String id;
}


