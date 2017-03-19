package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public interface IVertex<V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable> {
  Iterable<IEdge<E, I, J>> getOutEdges();

  I getVertexId();

  V getValue();

  void setValue(V value);

  boolean isRemote();

}
