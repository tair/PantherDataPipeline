package org.tair.module.phyloxml;

import org.tair.module.pantherForPhylo.Accession;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

@XmlAccessorType(XmlAccessType.FIELD)

public class Sequence {

    private Accession accession;

    public Accession getAccession() {
        return accession;
    }

    public void setAccession(Accession accession) {
        this.accession = accession;
    }
}
