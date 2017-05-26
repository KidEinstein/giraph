package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.giraph.master.SubgraphMasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Created by anirudh on 20/05/17.
 */
public class GiraphFlatTextInputSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends GiraphSubgraphComputation<S, I, V, E, M, SV, EI> {

  @Override
  public void compute(Vertex vertex, Iterable iterable) throws IOException {
    super.compute(vertex, iterable);
  }

  @Override
  public long getSuperstep() {
    return super.getSuperstep() + 3;
  }
}
