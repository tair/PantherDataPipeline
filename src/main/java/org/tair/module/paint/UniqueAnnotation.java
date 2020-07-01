package org.tair.module.paint;

import lombok.Data;

@Data
public class UniqueAnnotation {
    private String persistent_id;
    private String term;
    private String evidence_code;
    private String evidence_type;
    private String evidence_value;
    private String term_name;
    private String term_aspect;
}
