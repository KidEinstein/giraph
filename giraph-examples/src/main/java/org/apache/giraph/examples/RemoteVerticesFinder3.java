package org.apache.giraph.examples;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 06/11/16.
 */
public class RemoteVerticesFinder3 extends SubgraphComputation<LongWritable,p
    LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<SubgraphMessage<LongWritable, BytesWritable>> messages) throws IOException {
    HashMap<LongWritable, RemoteSubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> remoteVertices = new HashMap<>();
    //System.out.println("IN RVF 3\n");
    for (SubgraphMessage<LongWritable, BytesWritable> message : messages) {
      SubgraphId<LongWritable> senderSubgraphId = new SubgraphId<>();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getMessage().getBytes());
      senderSubgraphId.readFields(dataInput);
      //System.out.println("Message received from subgraph  ID :" + senderSubgraphId.getSubgraphId() + "to subgraph :"+subgraph.getId().getSubgraphId());
      int numVertices = dataInput.readInt();
      //System.out.println("numvertices received in this message are : "+ numVertices);
      for (int i = 0; i < numVertices; i++) {
        DefaultRemoteSubgraphVertex rsv = new DefaultRemoteSubgraphVertex();
        LongWritable rsvId = new LongWritable();
        rsvId.readFields(dataInput);
        //System.out.println("Remote Edge: From subgraph " + subgraph.getId().getSubgraphId() + " is To vertex : " + rsvId +" in neighbor subgraph with ID: " + senderSubgraphId);
        rsv.setSubgraphId(rsvId);
        remoteVertices.put(rsvId, rsv);
      }
    }
    subgraph.setRemoteVertices(remoteVertices);
  }
}


