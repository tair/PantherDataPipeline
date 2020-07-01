package org.tair.module.paint;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class PaintAnnotationData {
    private PaintAnnotationSearch search;

    //One unique annotation is counted as “term + evidence_code + evidence_type/evidence_value”
    public List<UniqueAnnotation> getUniqueAnnotations() {
        List<UniqueAnnotation> uniqueAnnos = new ArrayList<>();
//        System.out.println(search);
        if(search.getAnnotation_list() == null) return uniqueAnnos;

        List<PaintAnnotation> paintAnnotationList = search.getAnnotation_list().getAnnotation();
        paintAnnotationList.forEach(pa -> {
            List<Evidence> evidences = pa.getEvidence_list().getEvidence();
//            System.out.println(pa.getTerm() + evidences.size());
            evidences.forEach(evi -> {
                UniqueAnnotation uniqueAnnotation = new UniqueAnnotation();
                uniqueAnnotation.setPersistent_id(search.getParameters().getId());
                uniqueAnnotation.setTerm(pa.getTerm());
                uniqueAnnotation.setTerm_aspect(pa.getTerm_aspect());
                uniqueAnnotation.setTerm_name(pa.getTerm_name());
                uniqueAnnotation.setEvidence_code(evi.getEvidence_code());
                uniqueAnnotation.setEvidence_type(evi.getEvidence_type());
                uniqueAnnotation.setEvidence_value(evi.getEvidence_value());
                uniqueAnnos.add(uniqueAnnotation);
            });
        });
//        System.out.println("uniqueAnnos: "+ uniqueAnnos.size());
        return uniqueAnnos;
    }
}



@Data
class PaintAnnotationSearch {
    private PaintAnnotationList annotation_list;
    @JsonProperty("VERSION")
    private PaintVersion version;
    private PaintParams parameters;
}

@Data
class PaintAnnotationList {
    private List<PaintAnnotation> annotation;
}

@Data class PaintVersion {
    @JsonProperty("VERSION_GO")
    private String version_go;
    @JsonProperty("RELEASE_DATE_GO")
    private String release_date_go;
    @JsonProperty("VERSION_PANTHER")
    private String version_panther;
    @JsonProperty("RELEASE_DATE_PANTHER")
    private String release_data_panther;
}

@Data class PaintParams {
    private String id;
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class PaintAnnotation {
    private String term_name;
    private String term_aspect;
    private String term;
    private EvidenceList evidence_list;
}

@Data
//@JsonIgnoreProperties(ignoreUnknown = true)
class EvidenceList {
    private List<Evidence> evidence;
}

@Data
class Evidence {
    private String evidence_type;
    private String evidence_value;
    private String evidence_code;
}
