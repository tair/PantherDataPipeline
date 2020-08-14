package org.tair.module.paint.flatfile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
public class GoBasic {
    private List<Graphs> graphs;

    public List<Nodes> getNodes() {
        Graphs graphs = this.graphs.get(0);
        return graphs.getNodes();
    }
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Graphs {
    private String id;
    private List<Nodes> nodes;
    private List<Edges> edges;
}


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Edges {

}
