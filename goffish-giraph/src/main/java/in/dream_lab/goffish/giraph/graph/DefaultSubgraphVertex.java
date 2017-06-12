package in.dream_lab.goffish.giraph.graph;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 */
public class DefaultSubgraphVertex<V extends Writable, E extends Writable, I extends WritableComparable,
    J extends WritableComparable> implements IVertex<V, E, I, J>, WritableComparable {

    private static final Logger LOG  = Logger.getLogger(DefaultSubgraphVertex.class);

  private I id;
  private V value;

  public void setOutEdges(LinkedList<IEdge<E, I, J>> outEdges) {
    this.outEdges = outEdges;
  }

  private LinkedList<IEdge<E, I, J>> outEdges;

  @Override
  public LinkedList<IEdge<E, I, J>> getOutEdges() {
    return outEdges;
  }

  @Override
  public int compareTo(Object o) {
    DefaultSubgraphVertex other = (DefaultSubgraphVertex) o;
    return id.compareTo(other.id);
  }

  public I getId() {
    return id;
  }

  @Override
  public I getVertexId() {
    return id;
  }

  public void setId(I id) {
    this.id = id;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public boolean isRemote() {
    return false;
  }

  public void initialize(I vertexId, V value, LinkedList<IEdge<E, I, J>> edges) {
    this.id = vertexId;
    this.value = value;
    this.outEdges = edges;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    LOG.debug("SGVWrite: " + "ID:" + id + id.getClass().getSimpleName());
    LOG.debug("SGVWrite: " + "Value:" + value + value.getClass().getSimpleName());
    id.write(dataOutput);
    value.write(dataOutput);
    int numOutEdges = outEdges.size();
    dataOutput.writeInt(numOutEdges);
        LOG.debug("SGVWrite:Number edges: " + numOutEdges);
    for (IEdge<E, I, J> edge : outEdges) {
            LOG.debug("SGVWrite: " + "Edge,SRC" +id+",Sink,"+ edge.getSinkVertexId()+",Value,"+edge.getValue() + " Class: " + edge.getSinkVertexId().getClass().getSimpleName());
      edge.getSinkVertexId().write(dataOutput);
      //FIXME:edgevalue is null
      edge.getValue().write(dataOutput);
    }
    LOG.debug("SGVWVertex ID Class,VertexValueClass:" + id.getClass() + "," + value.getClass());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new NotImplementedException("Use read fields with GiraphSubgraphConfiguration as parameter");
  }

  public void readFields(GiraphSubgraphConfiguration<?, I, V, E, ?, J> conf, DataInput dataInput) throws IOException {
    // Getting the subgraph vertex internals
    id = conf.createSubgraphVertexId();
    value = conf.createSubgraphVertexValue();

    id.readFields(dataInput);
    value.readFields(dataInput);
    LOG.debug("SGVRRead ID:" + id + "\t"+ id.getClass().getSimpleName());
    LOG.debug("SGVRRead: " + "Value:" + value + value.getClass().getSimpleName());

    int numEdges = dataInput.readInt();
    LOG.debug("SGVRead: " + "Number edges: " + numEdges);
    outEdges = Lists.newLinkedList();
    for (int i = 0; i < numEdges; i++) {
//      LOG.debug("\n THIS IS I :  "+ i);
      DefaultSubgraphEdge<I, E, J> se = new DefaultSubgraphEdge<>();
      I targetId = conf.createSubgraphVertexId();
      E edgeValue = conf.createEdgeValue();
      targetId.readFields(dataInput);
      edgeValue.readFields(dataInput);
      se.initialize(null, edgeValue, targetId);
//        se.initialize(null, null, targetId);
      LOG.debug("SGVRead: " + "Edge,SRC,"+id+",Sink," + se.getSinkVertexId() +",Value,"+edgeValue +" Class: " + se.getSinkVertexId().getClass().getSimpleName());
      outEdges.add(se);
    }
  }






}
