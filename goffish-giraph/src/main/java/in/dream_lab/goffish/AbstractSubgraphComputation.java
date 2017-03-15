package in.dream_lab.goffish;

import in.dream_lab.goffish.giraph.GiraphSubgraphComputation;
import in.dream_lab.goffish.giraph.Subgraph;
import in.dream_lab.goffish.giraph.SubgraphId;
import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Created by anirudh on 26/02/17.
 */
public abstract class AbstractSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable>
    implements GiraphConfigurationSettable {

  private ImmutableClassesGiraphConfiguration conf;

  private ISubgraphPlatformCompute<S, I, V, E, M, SV, EI> subgraphPlatformCompute;

  public long getSuperstep() {
    return subgraphPlatformCompute.getSuperstep();
  }

  public void setSubgraphPlatformCompute(GiraphSubgraphComputation<S, I, V, E, M, SV, EI> subgraphPlatformCompute) {
    this.subgraphPlatformCompute = subgraphPlatformCompute;
  }

  public Subgraph<S, I, V, E, SV, EI> getSubgraph() {
    return subgraphPlatformCompute.getSubgraph();
  }

  public void voteToHalt() {
    subgraphPlatformCompute.voteToHalt();
  }

  public abstract void compute(Iterable<SubgraphMessage<S, M>> messages) throws IOException;

  public void sendMessage(SubgraphId<S> subgraphId, M message) {
    subgraphPlatformCompute.sendMessage(subgraphId, message);
  }

  public void sendToNeighbors(M message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    conf = configuration;
  }

  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  void sendMessage(SubgraphId<S> subgraphID, Iterable<M> message) {
    throw new UnsupportedOperationException();
  }


  void sendToAll(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  void sendToNeighbors(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

}
