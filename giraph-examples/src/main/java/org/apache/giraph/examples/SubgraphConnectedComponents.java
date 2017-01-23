package org.apache.giraph.examples;

import org.apache.giraph.graph.Subgraph;
import org.apache.giraph.graph.SubgraphComputation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Created by anirudh on 21/11/16.
 */
public class SubgraphConnectedComponents extends SubgraphComputation<LongWritable,
    LongWritable, NullWritable, NullWritable, LongWritable, LongWritable, NullWritable> {

  @Override
  public void compute(Subgraph<LongWritable, LongWritable, NullWritable, NullWritable, LongWritable, NullWritable> subgraph, Iterable<LongWritable> messages) throws IOException {
    if (getSuperstep() == 3) {
      LongWritable sid = subgraph.getId().getSubgraphId();
      subgraph.getSubgraphVertices().setSubgraphValue(sid);
      sendMessageToAllEdges(subgraph, sid);
    } else {
      long myMin = subgraph.getSubgraphVertices().getSubgraphValue().get();
      long currentMin = myMin;
      //System.out.println("My Min: " + myMin);
      for (LongWritable message : messages) {
        long neighborMin = message.get();
        //System.out.println("Message from neighbor: " + neighborMin);
        if (neighborMin < currentMin) {
          currentMin = neighborMin;
        }
      }
      if (currentMin < myMin) {
        subgraph.getSubgraphVertices().setSubgraphValue(new LongWritable(currentMin));
        sendMessageToAllEdges(subgraph, new LongWritable(currentMin));
      }
    }
    subgraph.voteToHalt();
  }
}
//TODO : we cant have member variables here( which wont be shared with other subgraphs)



