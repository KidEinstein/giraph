package in.dream_lab.goffish.giraph;

import in.dream_lab.goffish.AbstractSubgraphComputation;
import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 08/03/17.
 */
public class GiraphSandbox extends AbstractSubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  @Override
  public void compute(Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
    Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph = getSubgraph();
    for (SubgraphVertex subgraphVertex : subgraph.getLocalVertices()) {
      System.out.println("Vertex: " + subgraphVertex.getId());
      LinkedList<SubgraphEdge> outEdges = subgraphVertex.getOutEdges();
      for (SubgraphEdge subgraphEdge : outEdges) {
        System.out.println("Edges: " + subgraphEdge.getSinkVertexId());
      }
    }
    System.out.println("Printing remote");
    for (SubgraphVertex subgraphVertex : subgraph.getRemoteVertices()) {
      System.out.println("Remote Vertex: " + subgraphVertex.getId());
    }
    voteToHalt();
  }
}
