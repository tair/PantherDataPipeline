package org.tair.module.panther;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
//@JsonInclude(JsonInclude.Include.NON_NULL)
public class MSAList {
    private List<MSASequenceInfo> sequence_info;
}

