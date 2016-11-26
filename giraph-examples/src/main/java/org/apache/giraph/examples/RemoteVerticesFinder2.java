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
public class RemoteVerticesFinder2 extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, Text, NullWritable, LongWritable> {
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<Text> messages) throws IOException {
    HashSet<LongWritable> vertexHashSet = new HashSet<>();
    SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    System.out.println("Subgraph ID: " + subgraph.getId().getSubgraphId());
    System.out.println("SV: " + subgraphVertices);
    System.out.println("SV Linked List: " + subgraphVertices.getVertices());
    for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> sv : subgraphVertices.getVertices().values()) {
      vertexHashSet.add(sv.getId());
    }
    for (Text message : messages) {
      LinkedList<LongWritable> vertexIdsFound = new LinkedList();
      Text t = new Text();
      ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();
      SubgraphId<LongWritable> senderSubgraphId = new SubgraphId<>();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getBytes());
      senderSubgraphId.readFields(dataInput);
      System.out.println("Sender subgraphID for each message is : " + senderSubgraphId);
      int numVertices = dataInput.readInt();
      System.out.println("Sender number of vertices for each message is : " + numVertices);

      for (int i = 0; i < numVertices; i++) {
        LongWritable vertexId = new LongWritable();
        vertexId.readFields(dataInput);
        if (vertexHashSet.contains(vertexId)) {
          vertexIdsFound.add(vertexId);
        }
      }
      if (!vertexIdsFound.isEmpty()) {
        subgraph.getId().write(dataOutput);
        dataOutput.writeInt(vertexIdsFound.size());
        for (LongWritable found : vertexIdsFound) {
          found.write(dataOutput);
        }
        t.set(dataOutput.getByteArray());
        sendMessage(senderSubgraphId, t);
      }
    }
  }
}

