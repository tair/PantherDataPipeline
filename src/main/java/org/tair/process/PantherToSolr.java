package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.tair.module.Children;
import org.tair.module.panther.Annotation;
import org.tair.module.PantherData;

import java.util.ArrayList;
import java.util.List;

public class PantherToSolr {
    PantherData pantherData = null;
    private List<Annotation> annotations = null;

    public PantherData addSolrFields(PantherData orig) throws Exception {
        this.annotations = new ArrayList<Annotation>();
        String jsonString = orig.getJsonString();
        // convert json string to Panther object
        PantherData pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
                PantherData.class);
        this.pantherData = pantherData;
        this.pantherData.setId(orig.getId());
        this.pantherData.setJsonString(orig.getJsonString());

        try {
            if(pantherData.getSearch() == null) {
                System.out.println("Error in: " + pantherData.getId());
            } else {
                Annotation rootNodeAnnotation = pantherData.getSearch().getAnnotation_node();
                if(rootNodeAnnotation == null) {
//                    pantherData = null;
                } else {
                    System.out.println(rootNodeAnnotation);
                    addToListFromAnnotation(rootNodeAnnotation);
                    flattenTree(this.pantherData.getSearch().getAnnotation_node().getChildren());
                    buildListItems();
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Error in building list: " + e.getMessage());
        }
        return this.pantherData;
    }

    //PantherData has a list of unique variables which solr uses to index and search from and return the panther ID matching the search
    private void addToListFromAnnotation(Annotation annotation) {
        //Internal Node Variables
        //"accession"
//		if (annotation.getAccession() != null && !this.pantherData.getAccessions().contains(annotation.getAccession()))
//			this.pantherData.getAccessions().add(annotation.getAccession());
        //"public_id" - not indexed
        //"sf_id"
        if (annotation.getSf_id() != null && !this.pantherData.getSf_ids().contains(annotation.getSf_id()))
            this.pantherData.getSf_ids().add(annotation.getSf_id());
        //"sf_name"
        if (annotation.getSf_name() != null && !this.pantherData.getSf_names().contains(annotation.getSf_name()))
            this.pantherData.getSf_names().add(annotation.getSf_name());
        //"prop_sf_id" - not indexed
        //"species" - only add the top species from the list
        if (annotation.getSpecies() != null
                && !this.pantherData.getSpecies_list().contains(annotation.getSpecies())
                && this.pantherData.getSpecies_list().size() == 0)
            this.pantherData.getSpecies_list().add(annotation.getSpecies());
        //"taxonomic_range"
        if (annotation.getTaxonomic_range() != null && !this.pantherData.getTaxonomic_ranges().contains(annotation.getTaxonomic_range()))
            this.pantherData.getTaxonomic_ranges().add(annotation.getTaxonomic_range());
        //"tree_node_type" - not indexed
        //"event_type" = not indexed
        //"branch_length"
        if (annotation.getBranch_length() != null && !this.pantherData.getBranch_lengths().contains(annotation.getBranch_length()))
            this.pantherData.getBranch_lengths().add(annotation.getBranch_length());
        //"gene_id"
        if (annotation.getGene_id() != null && !this.pantherData.getGene_ids().contains(annotation.getGene_id()))
            this.pantherData.getGene_ids().add(annotation.getGene_id());
        //"gene_symbol"
        if (annotation.getGene_symbol() != null && !this.pantherData.getGene_symbols().contains(annotation.getGene_symbol()))
            this.pantherData.getGene_symbols().add(annotation.getGene_symbol());
        //"definition"
        if (annotation.getDefinition() != null && !this.pantherData.getDefinitions().contains(annotation.getDefinition()))
            this.pantherData.getDefinitions().add(annotation.getDefinition());
        //"node_name" -> "uniprotId"
        if (annotation.getNode_name() != null && !this.pantherData.getNode_names().contains(annotation.getNode_name())) {
            this.pantherData.getNode_names().add(annotation.getNode_name());
            // DICDI|dictyBase=DDB_G0277745|UniProtKB=Q86KT5
            int pos = annotation.getNode_name().indexOf("UniProtKB");
            String uniProtId = null;
            if(pos != -1) {
                uniProtId = annotation.getNode_name().substring(pos + 10);
                if(!this.pantherData.getUniprot_ids().contains(uniProtId))
                    this.pantherData.getUniprot_ids().add(uniProtId);
            }
        }
        //"organism"
        if (annotation.getOrganism() != null && !this.pantherData.getOrganisms().contains(annotation.getOrganism()))
            this.pantherData.getOrganisms().add(annotation.getOrganism());
    }

    private void flattenTree(Children children) {

        if (children == null || children.getAnnotation_node() == null || children.getAnnotation_node().size() == 0)
            return;

        for (Annotation annotation : children.getAnnotation_node()) {
            this.annotations.add(annotation);
            flattenTree(annotation.getChildren());
        }
    }

    private void buildListItems() {

        for (Annotation annotation : this.annotations) {
            addToListFromAnnotation(annotation);
        }
    }
}
