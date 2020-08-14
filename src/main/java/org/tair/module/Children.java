package org.tair.module;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;
import org.tair.module.panther.Annotation;

import java.util.List;

@Data
@JsonInclude(Include.NON_NULL)
public class Children {
	private List<Annotation> annotation_node;
}
