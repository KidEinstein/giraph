package org.apache.giraph.examples;

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
public class RemoteVerticesFinder2 extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<BytesWritable> messages) throws IOException {
    SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    //System.out.println("RVF2 Subgraph ID: " + subgraph.getId().getSubgraphId());
    int msgcount=0;
    HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> vertices = subgraphVertices.getVertices();
    for (BytesWritable message : messages) {
      msgcount++;
      LinkedList<LongWritable> vertexIdsFound = new LinkedList();
      ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();
      SubgraphId<LongWritable> senderSubgraphId = new SubgraphId<>();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getBytes());
      senderSubgraphId.readFields(dataInput);
      //System.out.println("Sender subgraphID for each message is : " + senderSubgraphId);
      int numVertices = dataInput.readInt();
      //System.out.println("Sender number of vertices for each message is : " + numVertices);

      for (int i = 0; i < numVertices; i++) {
        LongWritable vertexId = new LongWritable();
        vertexId.readFields(dataInput);
        if (vertices.containsKey(vertexId)) {
          vertexIdsFound.add(vertexId);
        }
      }
      if (!vertexIdsFound.isEmpty()) {
        subgraph.getId().write(dataOutput);
        dataOutput.writeInt(vertexIdsFound.size());
        for (LongWritable found : vertexIdsFound) {
          found.write(dataOutput);
        }
        BytesWritable bw = new BytesWritable(dataOutput.getByteArray());
        sendMessage(senderSubgraphId, bw);
      }
    }
    //System.out.println("for subgraph id : "+subgraph.getId().getSubgraphId() + " incoming messages in RVF2 are :" +msgcount);

  }
}

