package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 23/10/16.
 */
public interface Subgraph<S extends WritableComparable,
    I extends WritableComparable, V extends Writable,
    E extends Writable, SV extends Writable, EI extends WritableComparable>
    extends Vertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> {

  SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices();

}
