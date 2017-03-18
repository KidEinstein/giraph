package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 02/11/16.
 */
public interface RemoteSubgraphVertex<V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable>
    extends SubgraphVertex<V, E, I, J> {
  K getSubgraphId();
  V getLocalState();
  void setLocalState(V value);
}
