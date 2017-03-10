package in.dream_lab.goffish;

import in.dream_lab.goffish.giraph.Subgraph;
import in.dream_lab.goffish.giraph.SubgraphId;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 09/03/17.
 */
public interface ISubgraphPlatformCompute<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> {
  Subgraph<S, I, V, E, SV, EI> getSubgraph();//templatize return type, G extends ISubgraph<S, V, E, I, J, K>

  void voteToHalt();

  long getSuperstep();

  void sendMessage(SubgraphId<S> subgraphId, M message);

  void sendToVertex(I vertexID, M message);

  void sendToAll(M message); // auto fill subgraph ID on send or receive

  void sendToNeighbors(M message);

  void sendMessage(SubgraphId<S> subgraphId, Iterable<M> message);

  void sendToAll(Iterable<M> message);

  void sendToNeighbors(Iterable<M> message);
}
