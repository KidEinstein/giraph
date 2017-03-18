package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Created by anirudh on 26/02/17.
 */
public abstract class AbstractSubgraphComputation<S extends Writable, V extends WritableComparable, E extends Writable, M extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> {

  private ISubgraphPlatformCompute<S, V, E, M, I, J, K> subgraphPlatformCompute;

  public long getSuperstep() {
    return subgraphPlatformCompute.getSuperstep();
  }

  public void setSubgraphPlatformCompute(ISubgraphPlatformCompute<S, V, E, M, I, J, K> subgraphPlatformCompute) {
    this.subgraphPlatformCompute = subgraphPlatformCompute;
  }

  public Subgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraphPlatformCompute.getSubgraph();
  }

  public void voteToHalt() {
    subgraphPlatformCompute.voteToHalt();
  }

  public abstract void compute(Iterable<IMessage<K,M>> messages) throws IOException;

  public void sendMessage(K subgraphId, M message) {
    subgraphPlatformCompute.sendMessageToSubgraph(subgraphId, message);
  }

  public void sendToNeighbors(M message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }

  public String getConf(String key) {
    return subgraphPlatformCompute.getConf(key);
  }

  void sendMessage(K subgraphID, Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  void sendToAll(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  void sendToNeighbors(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }
}
