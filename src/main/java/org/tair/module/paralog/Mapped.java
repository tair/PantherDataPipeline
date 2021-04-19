package org.tair.module.paralog;

import lombok.Data;

@Data
public class Mapped {
    private String target_gene_symbol;
    private String species_after_duplication;
    private String persistent_id;
    private String target_persistent_id;
    private String ortholog;
    private String gene;
    private String taxon_preceding_duplication;
    private String target_gene;
    private String target_uniprot;
    private String target_agi;
    private String id;
    private String species_preceding_duplication;
    private String taxon_after_duplication;
}
