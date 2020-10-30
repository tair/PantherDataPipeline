package org.tair.controller;

public class OrthoObj {
    private String uniprotId;
    private String queryOrganismId;

    public String getUniprotId() {
        return this.uniprotId;
    }

    public void setUniprotId(String id) {
        this.uniprotId = id;
    }

    public String getQueryOrganismId() { return this.queryOrganismId; }

    public void setQueryOrganismId(String id) {this.queryOrganismId = id; }
}
