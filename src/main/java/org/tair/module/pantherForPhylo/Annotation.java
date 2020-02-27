package org.tair.module.pantherForPhylo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Annotation {
    private String node_type;
    private String event_type;
    private String gene_symbol;
    private String gene_id;
    private String node_name;
    private String organism;
    private String definition;
    private String branch_length;
    private String accession;
    private String sf_name;
    private String sf_id;
    private String speciation_event;
    private String reference_speciation_event;
    private String species;
    private String PANTHER_GO_SLIM_CC;
    private Children children;


    public String getEvent_type() {
        return event_type;
    }

    public Children getChildren() {
        return children;
    }

    public String getAccession() {
        return accession;
    }

    public String getBranch_length() {
        return branch_length;
    }

    public String getSf_id() {
        return sf_id;
    }

    public String getSf_name() {
        return sf_name;
    }

    public String getSpeciation_event() {
        return speciation_event;
    }

    public String getSpecies() {
        return species;
    }

    public String getNode_name() {
        return node_name;
    }

    public String getNode_type() {
        return node_type;
    }

    public String getGene_id() {
        return gene_id;
    }

    public String getGene_symbol() {
        return gene_symbol;
    }

    public String getDefinition() {
        return definition;
    }

    public String getOrganism() {
        return organism;
    }

    public String getPANTHER_GO_SLIM_CC() {
        return PANTHER_GO_SLIM_CC;
    }

    public String getReference_speciation_event() {
        return reference_speciation_event;
    }
}
