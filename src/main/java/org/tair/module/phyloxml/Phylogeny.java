package org.tair.module.phyloxml;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlType (propOrder={"name","description","clade"})
public class Phylogeny implements Serializable {
    private Clade clade;
    private String name;  //PANTHER_family_name
    private String description; // PANTHER_family_ID or book

    private String rooted;

    @XmlAttribute
    public String getRooted() {
        return rooted;
    }

    public void setRooted(String rooted) {
        this.rooted = rooted;
    }

    public Clade getClade() {
        return clade;
    }

    public String getDescription() {
        return description;
    }

    public String getName() {
        return name;
    }

    public void setClade(Clade clade) {
        this.clade = clade;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setName(String name) {
        this.name = name;
    }
}

