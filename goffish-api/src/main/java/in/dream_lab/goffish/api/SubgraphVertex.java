package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public interface SubgraphVertex<V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable> {
  LinkedList<SubgraphEdge<E, I, J>> getOutEdges();

  I getId();

  void setId(I id);

  V getValue();

  void setValue(V value);

  boolean isRemote();

  void initialize(I vertexId, V value, LinkedList<SubgraphEdge<E, I, J>> edges);
}
