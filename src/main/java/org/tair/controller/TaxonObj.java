package org.tair.controller;

import java.util.List;

public class TaxonObj {
    private List<String> taxonIdsToShow;

    public List<String> getTaxonIdsToShow() {
        return taxonIdsToShow;
    }

    public void setTaxonIdsToShow(List<String> ids) {
        this.taxonIdsToShow = ids;
    }
}
