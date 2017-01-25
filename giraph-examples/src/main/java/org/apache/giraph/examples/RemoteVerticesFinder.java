package org.apache.giraph.examples;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import static org.apache.giraph.utils.MemoryUtils.freeMemoryMB;

/**
 * Created by anirudh on 02/11/16.
 */
public class RemoteVerticesFinder extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  public static final Logger LOG = Logger.getLogger(RemoteVerticesFinder.class);
  @Override
  public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
    HashSet<LongWritable> vertexHashSet = new HashSet<>();
    SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    //System.out.println("SV in RVF 1 : " + subgraphVertices);
    HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> vertices = subgraphVertices.getVertices();
    //System.out.println("SV Linked List in 1 : " + vertices);

//    for (MemoryPoolMXBean mpBean: ManagementFactory.getMemoryPoolMXBeans()) {
//      if (mpBean.getType() == MemoryType.HEAP) {
//        System.out.printf(
//            "Test 1, Name: %s: %s\n",
//            mpBean.getName(), mpBean.getUsage()
//        );
//      }
//    }
    LOG.info("Test 1, Free memory: " + freeMemoryMB());

    HashSet<LongWritable> remoteVertexIds = new HashSet<>();

    ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();

    for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> sv : vertices.values()) {
      //LOG.info("Test, Number of vertex edges: " + sv.getOutEdges().size());
      for (SubgraphEdge<LongWritable, DoubleWritable, LongWritable> se : sv.getOutEdges()) {
        //System.out.println("Subgraph ID  : " + subgraph.getId().getSubgraphId() +"\t its vertex : " + sv.getId() + " has edge pointing to " + se.getSinkVertexId()+"\n");

        if (!vertices.containsKey(se.getSinkVertexId())) {
          //System.out.println("Parent subgraph " + subgraph.getId().getSubgraphId() +"does not contain the vertex id  : " + se.getSinkVertexId());
          remoteVertexIds.add(se.getSinkVertexId());
        }
      }
    }

    subgraph.getId().write(dataOutput);
//    LOG.info("Test, Sender subgraphID is : " + subgraph.getId());
    dataOutput.writeInt(remoteVertexIds.size());
//    LOG.info("Test, Sender number of remote vertices are  : " + remoteVertexIds.size());
//    LOG.info("Test, Number of edges: " + subgraph.getNumEdges());
//    LOG.info("Test, Number of  vertices are " + vertices.size());

    for (LongWritable remoteSubgraphVertexId : remoteVertexIds) {
      remoteSubgraphVertexId.write(dataOutput);
    }
    BytesWritable bw = new BytesWritable(dataOutput.getByteArray());

//    LOG.info("Test, DataOutput size " + dataOutput.size());
//
//    for (MemoryPoolMXBean mpBean: ManagementFactory.getMemoryPoolMXBeans()) {
//      if (mpBean.getType() == MemoryType.HEAP) {
//        System.out.printf(
//            "Test 2, Name: %s: %s\n",
//            mpBean.getName(), mpBean.getUsage()
//        );
//      }
//    }
    LOG.info("Test 2, Free memory: " + freeMemoryMB());

    // LOG.info("Test, All messages sent");
    sendMessageToAllNeighboringSubgraphs(subgraph, bw);
  }
}
