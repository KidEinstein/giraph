package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface Subgraph<S extends WritableComparable, I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable> extends Vertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI> , E>{
    Iterable<Edge<SubgraphId<S>, E>> getRemoteEdges();

    Iterable<SubgraphVertex<S, I, Writable, E, EI>> getRemoteVertices();

    SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices();
}
