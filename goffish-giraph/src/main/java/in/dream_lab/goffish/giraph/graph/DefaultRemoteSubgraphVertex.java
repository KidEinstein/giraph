package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public class DefaultRemoteSubgraphVertex<V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable>
    extends DefaultSubgraphVertex<V, E, I, J> implements IRemoteVertex<V, E, I, J, K> {

  private K subgraphId;

  private V localState;

  public void initialize(K subgraphId, I vertexId, V value, LinkedList<IEdge<E, I, J>> subgraphEdges) {
    this.subgraphId = subgraphId;
    super.initialize(vertexId, value, subgraphEdges);
  }

  @Override
  public boolean isRemote() {
    return true;
  }

  @Override
  public K getSubgraphId() {
    return subgraphId;
  }

  @Override
  public V getLocalState() {
    return localState;
  }

  @Override
  public void setLocalState(V value) {
    localState = value;
  }

  public void setSubgraphId(K subgraphId) {
    this.subgraphId = subgraphId;
  }

  @Override
  public void readFields(GiraphSubgraphConfiguration<?, I, V, E, ?, J> conf, DataInput dataInput) throws IOException {
    super.readFields(conf, dataInput);
    subgraphId = (K) conf.createSubgraphId();
    subgraphId.readFields(dataInput);
    localState = conf.createSubgraphVertexValue();
    localState.readFields(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    subgraphId.write(dataOutput);
    localState.write(dataOutput);
  }

  //  @Override
//  public V getValue() {
//    throw new UnsupportedOperationException("getValue() not supported for remote vertices");
//  }
}
