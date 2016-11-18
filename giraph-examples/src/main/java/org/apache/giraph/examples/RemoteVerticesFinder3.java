package org.apache.giraph.examples;

import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 06/11/16.
 */
public class RemoteVerticesFinder3 extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, Text, NullWritable, LongWritable> {
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<Text> messages) throws IOException {
    LinkedList<RemoteSubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> remoteList = new LinkedList<>();
    System.out.println("TAZOTAZO");
    for (Text message : messages) {
      System.out.println("KUTTAKUTTA");
      SubgraphId<LongWritable> senderSubgraphId = new SubgraphId<>();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getBytes());
      senderSubgraphId.readFields(dataInput);
      System.out.println("Received from subgraph:" + senderSubgraphId.getSubgraphId());
      int numVertices = dataInput.readInt();
      for (int i = 0; i < numVertices; i++) {
        DefaultRemoteSubgraphVertex rsv = new DefaultRemoteSubgraphVertex();
        LongWritable rsvId = new LongWritable();
        rsvId.readFields(dataInput);
        rsv.initialize(senderSubgraphId, rsvId, null, null);
        remoteList.add(rsv);
        System.out.println("Remote Edge: From" + subgraph.getId().getSubgraphId() + " To: " + rsvId +" in " + senderSubgraphId);
      }
    }
    subgraph.setRemoteVertices(remoteList);
  }
}


