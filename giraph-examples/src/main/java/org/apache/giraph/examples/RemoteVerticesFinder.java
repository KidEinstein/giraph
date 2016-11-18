package org.apache.giraph.examples;

import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public class RemoteVerticesFinder extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, Text, NullWritable, LongWritable> {

  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<Text> messages) throws IOException {
    HashSet<LongWritable> vertexHashSet = new HashSet<>();
    SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    System.out.println("SV in 1 : " + subgraphVertices);
    System.out.println("SV Linked List in 1 : " + subgraphVertices.getVertices());
    for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> sv : subgraphVertices.getVertices()) {
      System.out.println("Subgraph Vertex: " + sv.getId());
      vertexHashSet.add(sv.getId());
    }

    LinkedList<LongWritable> remoteVertexIds = new LinkedList<>();

    Text t = new Text();
    ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();

    for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> sv : subgraphVertices.getVertices()) {

      for (SubgraphEdge<LongWritable, DoubleWritable, LongWritable> se : sv.getOutEdges()) {
        System.out.println("batman subgraph edges sinks  : " + se.getSinkVertexId());

        if (!vertexHashSet.contains(se.getSinkVertexId())) {
          System.out.println("batman parent subgraph not contains the vertex id  : " + se.getSinkVertexId());
          remoteVertexIds.add(se.getSinkVertexId());
        }
      }
    }

    subgraph.getId().write(dataOutput);
    System.out.println("Batman sender subgraphID is : " + subgraph.getId());
    dataOutput.writeInt(remoteVertexIds.size());
    System.out.println("batman sender number of remote vertices are  : " + remoteVertexIds.size());

    for (LongWritable remoteSubgraphVertexId : remoteVertexIds) {
      remoteSubgraphVertexId.write(dataOutput);
    }

    t.set(dataOutput.getByteArray());
    sendMessageToAllEdges(subgraph, t);
  }
}
