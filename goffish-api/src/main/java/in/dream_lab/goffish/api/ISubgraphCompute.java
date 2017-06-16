package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 09/03/17.
 */
public interface ISubgraphCompute<S extends Writable, V extends WritableComparable, E extends Writable, M extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> {
  ISubgraph<S, V, E, I, J, K> getSubgraph();//templatize return type, G extends ISubgraph<S, V, E, I, J, K>

  void voteToHalt();

  long getSuperstep();

  int getMyWorkerID();

  void sendMessageToSubgraph(K subgraphId, M message);

  void sendToVertex(I vertexID, M message);

  void sendToAll(M message); // auto fill subgraph ID on send or receive

  void sendToNeighbors(M message);

  void sendMessage(K subgraphId, Iterable<M> messages);

  void sendToAll(Iterable<M> messages);

  void sendToNeighbors(Iterable<M> messages);

  String getConf(String key);

  boolean hasVotedToHalt();
}
