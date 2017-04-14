package in.dream_lab.goffish.giraph.graph;

import com.google.common.collect.Lists;
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
    // Getting the subgraph vertex internals
    I id = conf.createSubgraphVertexId();
    setId(id);
    id.readFields(dataInput);
    int numEdges = dataInput.readInt();
//    System.out.println("Read: " + "Number edges: " + numEdges);
    LinkedList<IEdge<E, I, J>> outEdges = Lists.newLinkedList();
    for (int i = 0; i < numEdges; i++) {
//      System.out.println("\n THIS IS I :  "+ i);
      DefaultSubgraphEdge<I, E, J> se = new DefaultSubgraphEdge<>();
      I targetId = conf.createSubgraphVertexId();
      E edgeValue = conf.createEdgeValue();
      targetId.readFields(dataInput);
      edgeValue.readFields(dataInput);
      se.initialize(null, edgeValue, targetId);
//      System.out.println("Read: " + "Edge:" + se.getSinkVertexId() + " Class: " + se.getSinkVertexId().getClass().getSimpleName());
      outEdges.add(se);
    }
    setOutEdges(outEdges);
    subgraphId = (K) conf.createSubgraphId();
    subgraphId.readFields(dataInput);
//    localState = conf.createSubgraphVertexValue();
//    localState.readFields(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    getId().write(dataOutput);
    int numOutEdges = getOutEdges().size();
    dataOutput.writeInt(numOutEdges);
//        System.out.println("Write: " + "Number edges: " + numOutEdges);
    for (IEdge<E, I, J> edge : getOutEdges()) {
//            System.out.println("Write: " + "Edge:" + edge.getSinkVertexId() + " Class: " + edge.getSinkVertexId().getClass().getSimpleName());
      edge.getSinkVertexId().write(dataOutput);
      edge.getValue().write(dataOutput);
    }
//    System.out.println("Vertex ID Class,VertexValueClass:" + id.getClass() + "," + value.getClass());
    subgraphId.write(dataOutput);
//    localState.write(dataOutput);
  }

  //  @Override
//  public V getValue() {
//    throw new UnsupportedOperationException("getValue() not supported for remote vertices");
//  }
}
