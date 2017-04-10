package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 18/10/16.
 */
public interface IEdge<E extends Writable, I extends WritableComparable, J extends WritableComparable> extends Writable {
  I getSinkVertexId();

  E getValue();

  J getEdgeId();

  void setValue(E value);
}
