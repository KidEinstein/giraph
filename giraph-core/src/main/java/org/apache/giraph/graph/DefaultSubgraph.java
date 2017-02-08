package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <SV> Subgraph Value type
 */
public class DefaultSubgraph<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable>
    extends DefaultVertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> implements Subgraph<S, I, V, E, SV, EI> {

  public void setRemoteVertices(HashMap<S, RemoteSubgraphVertex<S, I, V, E, EI>> remoteVertices) {
    SubgraphVertices<S, I, V, E, SV, EI> subgraphVertices = getValue();
    subgraphVertices.setRemoteVertices(remoteVertices);
  }

  @Override
  public SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices() {
    return getValue();
  }

}