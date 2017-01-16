package org.apache.giraph.examples;

import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public class RemoteVerticesFinder extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  public static final Logger LOG = Logger.getLogger(RemoteVerticesFinder.class);
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<BytesWritable> messages) throws IOException {
    SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    //System.out.println("SV in RVF 1 : " + subgraphVertices);
    HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> vertices = subgraphVertices.getVertices();
    //System.out.println("SV Linked List in 1 : " + vertices);
    HashSet<LongWritable> remoteVertexIds = new HashSet<>();

    ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();

    for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> sv : vertices.values()) {

      for (SubgraphEdge<LongWritable, DoubleWritable, LongWritable> se : sv.getOutEdges()) {
        //System.out.println("Subgraph ID  : " + subgraph.getId().getSubgraphId() +"\t its vertex : " + sv.getId() + " has edge pointing to " + se.getSinkVertexId()+"\n");

        if (!vertices.containsKey(se.getSinkVertexId())) {
          //System.out.println("Parent subgraph " + subgraph.getId().getSubgraphId() +"does not contain the vertex id  : " + se.getSinkVertexId());
          remoteVertexIds.add(se.getSinkVertexId());
        }
      }
    }

    subgraph.getId().write(dataOutput);
    LOG.info("Test, Sender subgraphID is : " + subgraph.getId());
    dataOutput.writeInt(remoteVertexIds.size());
    LOG.info("Test, Sender number of remote vertices are  : " + remoteVertexIds.size());
    LOG.info("Test, Number of edges: " + subgraph.getNumEdges());
    LOG.info("Test, Number of  vertices are " + vertices.size());

    for (LongWritable remoteSubgraphVertexId : remoteVertexIds) {
      remoteSubgraphVertexId.write(dataOutput);
    }

    BytesWritable bw = new BytesWritable(dataOutput.getByteArray());
    LOG.info("Test, DataOutput size " + dataOutput.size());

    sendMessageToAllEdges(subgraph, bw);

    LOG.info("Test, All messages sent");

  }
}
