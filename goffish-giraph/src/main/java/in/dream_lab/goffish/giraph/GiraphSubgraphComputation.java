package in.dream_lab.goffish.giraph;

import in.dream_lab.goffish.AbstractSubgraphComputation;
import in.dream_lab.goffish.ISubgraphPlatformCompute;
import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <M>  Message type
 * @param <SV> Subgraph Value type
 */


// S subgraph value type ----- SV now
// V vertex object type   -- V is the vertex value
// E edge value type    -- E is the edge value how
// M msg object type    -- M is the message value type
// I vertex id      --- I is the vertex id here
// J edge id        -- EI
// K subgraph id  ---- S

public class GiraphSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends BasicComputation<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E, SubgraphMessage<S, M>>
    implements ISubgraphPlatformCompute<S, I, V, E, M, SV, EI> {

  private static final Logger LOG = Logger.getLogger(GiraphSubgraphComputation.class);

  private static final ClassConfOption<AbstractSubgraphComputation> SUBGRAPH_COMPUTATION_CLASS = ClassConfOption.create("subgraphComputationClass",
      null, AbstractSubgraphComputation.class, "Subgraph Computation Class");

  private AbstractSubgraphComputation<S, I, V, E, M, SV, EI> abstractSubgraphComputation;

  // TODO: Have class be specified in conf

  @Override
  public void sendToVertex(I vertexID, M message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToAll(M message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendMessage(SubgraphId<S> subgraphId, Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToAll(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToNeighbors(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  DefaultSubgraph<S, I, V, E, SV, EI> subgraph;

  public Subgraph<S, I, V, E, SV, EI> getSubgraph() {
    return subgraph;
  }
  // TODO: Take care of state changes for the subgraph passed

  public void compute(Vertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> vertex, Iterable<SubgraphMessage<S, M>> messages) throws IOException {
    Class userSubgraphComputationClass;
    long superstep = super.getSuperstep();
    if (superstep == 0) {
      userSubgraphComputationClass = RemoteVerticesFinder.class;
    } else if (superstep == 1) {
      userSubgraphComputationClass = RemoteVerticesFinder2.class;
    } else if (superstep == 2) {
      userSubgraphComputationClass = RemoteVerticesFinder3.class;
    } else {
      userSubgraphComputationClass = SUBGRAPH_COMPUTATION_CLASS.get(getConf());
      LOG.info("User Class: " + userSubgraphComputationClass);
    }
    abstractSubgraphComputation = (AbstractSubgraphComputation<S, I, V, E, M, SV, EI>) ReflectionUtils.newInstance(userSubgraphComputationClass, getConf());
    LOG.info("User Object: " + abstractSubgraphComputation);
    abstractSubgraphComputation.setSubgraphPlatformCompute(this);
    subgraph = (DefaultSubgraph) vertex;
    abstractSubgraphComputation.compute(messages);
  }

  public void sendToNeighbors(M message) {
    WritableComparable subgraphId = subgraph.getSubgraphId();
    SubgraphMessage sm = new SubgraphMessage(subgraphId, message);
    super.sendMessageToAllEdges(subgraph, sm);
  }



  public void sendMessage(SubgraphId<S> subgraphId, M message) {
    SubgraphMessage sm = new SubgraphMessage(subgraphId.getSubgraphId(), message);
    sendMessage(subgraphId, sm);
  }

  public void voteToHalt() {
    subgraph.voteToHalt();
  }

  SubgraphEdge<I, E, EI> getEdgeById(EI id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getSuperstep() {
    return super.getSuperstep() - 3;
  }
}
