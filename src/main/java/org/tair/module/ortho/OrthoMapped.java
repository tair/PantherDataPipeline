package org.tair.module.ortho;

import lombok.Data;

@Data
public class OrthoMapped {
    private String target_gene_symbol;
    private String persistent_id;
    private String target_persistent_id;
    private String ortholog;
    private String gene;
    private String target_gene;
    private String uniprot_id;
    private String organism;
    private String target_gene_id;
    private String id;
    private String full_name;
    private String common_name;
    private String group_name;
}
