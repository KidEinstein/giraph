package org.apache.giraph.examples;

import com.google.common.primitives.Longs;
import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.Subgraph;
import org.apache.giraph.graph.SubgraphComputation;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by anirudh on 21/11/16.
 */
public class SubgraphConnectedComponents extends SubgraphComputation<LongWritable,
    LongWritable, NullWritable, NullWritable, BytesWritable, LongWritable, NullWritable> {

  @Override
  public void compute(Subgraph<LongWritable, LongWritable, NullWritable, NullWritable, LongWritable, NullWritable> subgraph, Iterable<SubgraphMessage<LongWritable, BytesWritable>> messages) throws IOException {
    if (getSuperstep() == 0) {
      LongWritable sid = subgraph.getId().getSubgraphId();
      subgraph.getSubgraphVertices().setSubgraphValue(sid);
      sendMessageToAllNeighboringSubgraphs(subgraph, new BytesWritable(Longs.toByteArray(sid.get())));
    } else {
      long myMin = subgraph.getSubgraphVertices().getSubgraphValue().get();
      long currentMin = myMin;
      //System.out.println("My Min: " + myMin);
      for (SubgraphMessage<LongWritable, BytesWritable> message : messages) {
        long neighborMin = Longs.fromByteArray(message.getMessage().getBytes());
        //System.out.println("Message from neighbor: " + neighborMin);
        if (neighborMin < currentMin) {
          currentMin = neighborMin;
        }
      }
      if (currentMin < myMin) {
        subgraph.getSubgraphVertices().setSubgraphValue(new LongWritable(currentMin));
        sendMessageToAllNeighboringSubgraphs(subgraph, new BytesWritable(Longs.toByteArray(currentMin)));
      }
    }
    subgraph.voteToHalt();
  }
}
//TODO : we cant have member variables here( which wont be shared with other subgraphs)



