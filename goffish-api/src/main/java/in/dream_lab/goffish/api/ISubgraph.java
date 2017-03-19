package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface ISubgraph<S extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> {

  IVertex<V, E, I, J> getVertexById(I vertexId);

  K getSubgraphId();

  long getVertexCount();

  long getLocalVertexCount();

  Iterable<IVertex<V, E, I, J>> getVertices();

  Iterable<IVertex<V, E, I, J>> getLocalVertices();

  Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices();

  Iterable<IEdge<E, I, J>> getOutEdges();

  IEdge<E, I, J> getEdgeById(J edgeId);

  void setSubgraphValue(S value);

  S getSubgraphValue();

}
