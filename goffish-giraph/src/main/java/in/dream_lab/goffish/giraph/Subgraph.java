package in.dream_lab.goffish.giraph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface Subgraph<S extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable
    > {
//TODO remove this
  SubgraphVertices<S, V, E, I, J, K> getSubgraphVertices();

  SubgraphVertex<V, E, I, J> getVertexById(I vertexId);

  K getSubgraphId();

  long getVertexCount();

  long getLocalVertexCount();

  Iterable<SubgraphVertex<V, E, I, J>> getVertices();

  Iterable<SubgraphVertex<V, E, I, J>> getLocalVertices();

  Iterable<RemoteSubgraphVertex<V, E, I, J, K>> getRemoteVertices();

  // TODO: Change function in API
  Iterable<SubgraphEdge<E, I, J>> getOutEdges();

  SubgraphEdge<E, I, J> getEdgeById(J edgeId);

  // TODO: Change function in API
  void setSubgraphValue(S value);

  S getSubgraphValue();

}
