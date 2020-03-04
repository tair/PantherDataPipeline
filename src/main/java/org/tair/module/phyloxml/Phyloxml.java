package org.tair.module.phyloxml;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "phyloxml")
public class Phyloxml {
    private Phylogeny phylogeny;

    public Phylogeny getPhylogeny() {
        return phylogeny;
    }

    public void setPhylogeny(Phylogeny phylogeny) {
        this.phylogeny = phylogeny;
    }
}
