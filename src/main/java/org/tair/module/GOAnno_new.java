package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GOAnno_new {
    private String geneProductId;
    private String goId;
    private String goName;
    private String goAspect;

    private String evidenceCode;
    private String reference;
}
