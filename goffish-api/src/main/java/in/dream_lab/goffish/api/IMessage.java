package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 17/03/17.
 */
public interface IMessage<S extends WritableComparable, M extends Writable> extends Writable {
  S getSubgraphId();

  M getMessage();
}
