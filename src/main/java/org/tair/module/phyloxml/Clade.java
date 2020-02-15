package org.tair.module.phyloxml;

import java.util.List;
import javax.xml.bind.annotation.XmlType;

@XmlType (propOrder={"name","branch_length","events","taxonomy","sequence","clade"})

public class Clade {
    private Events events;
    private String branch_length;
    private Taxonomy taxonomy;
    private Sequence sequence;
    private String name;
    private List<Clade> clade; // change to cladeChildren







    public String getBranch_length() {
        return branch_length;
    }

    public Events getEvents() {
        return events;
    }

    public List<Clade> getClade() {
        return clade;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public Taxonomy getTaxonomy() {
        return taxonomy;
    }

    public void setClade(List<Clade> clade) {
        this.clade = clade;
    }

    public void setBranch_length(String branch_length) {
        this.branch_length = branch_length;
    }

    public void setEvents(Events events) {
        this.events = events;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public void setTaxonomy(Taxonomy taxonomy) {
        this.taxonomy = taxonomy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
