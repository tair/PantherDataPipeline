package org.tair.module.paint.flatfile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Nodes {
    private String id;
    private String lbl;
    private Meta meta;
    public List<String> getGoAspects() {
        if(this.meta == null) return null;
        List<String> aspects = new ArrayList<>();
        if(this.meta.getBasicPropertyValues() != null && this.meta.getBasicPropertyValues().size() > 0) {
            for(int i=0; i<this.meta.getBasicPropertyValues().size(); i++) {
                aspects.add(this.meta.getBasicPropertyValues().get(i).getVal());
            }
            return aspects;
        }
        return aspects;
    }
    public String getGoId() {
        if(this.id.split("GO_").length < 2) return null;
        return this.id.split("GO_")[1];
    }
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Meta {
    private List<BasicValues> basicPropertyValues;
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class BasicValues {
    private String val;
}