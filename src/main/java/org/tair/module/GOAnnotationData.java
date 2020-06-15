package org.tair.module;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.beans.Field;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GOAnnotationData {
	@Field
	private String uniprot_id;
	@Field
	private String go_annotations;

	public static GOAnnotationData createFromGOAnnotation(GOAnnotation goAnnotation) throws JsonProcessingException {
		GOAnnotationData goAnnotationData = new GOAnnotationData();
		goAnnotationData.setUniprot_id(goAnnotation.getGeneProductId());
		goAnnotationData.setGo_annotations(new ObjectMapper().writer().writeValueAsString(goAnnotation));
		return goAnnotationData;
	}
}
