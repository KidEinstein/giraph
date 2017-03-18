package in.dream_lab.goffish.giraph;

import in.dream_lab.goffish.api.SubgraphEdge;
import in.dream_lab.goffish.api.SubgraphVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by anirudh on 02/11/16.
 */
public class RemoteVerticesFinder extends GiraphSubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  public static final Logger LOG = Logger.getLogger(RemoteVerticesFinder.class);

  @Override
  public void compute(Vertex<SubgraphId<LongWritable>, SubgraphVertices<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable>, DoubleWritable> vertex, Iterable<SubgraphMessage<LongWritable,BytesWritable>> subgraphMessages) throws IOException {
    DefaultSubgraph<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraph = (DefaultSubgraph) vertex;
    MapWritable subgraphPartitionMap = new MapWritable();
    subgraphPartitionMap.put(subgraph.getSubgraphId(), new IntWritable(subgraph.getPartitionId()));
    aggregate(SubgraphMasterCompute.ID, subgraphPartitionMap);
    SubgraphVertices<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();
    //System.out.println("SV in RVF 1 : " + subgraphVertices);
    HashMap<LongWritable, SubgraphVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable>> vertices = subgraphVertices.getLocalVertices();
    //System.out.println("SV Linked List in 1 : " + vertices);

//    for (MemoryPoolMXBean mpBean: ManagementFactory.getMemoryPoolMXBeans()) {
//      if (mpBean.getType() == MemoryType.HEAP) {
//        System.out.printf(
//            "Test 1, Name: %s: %s\n",
//            mpBean.getName(), mpBean.getUsage()
//        );
//      }
//    }
    //LOG.info("Test 1, Free memory: " + freeMemoryMB());

    HashSet<LongWritable> remoteVertexIds = new HashSet<>();

    ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();

    int edgeCount = 0;

    for (SubgraphVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable> sv : vertices.values()) {
      edgeCount += sv.getOutEdges().size();
      // LOG.info("Test, Number of vertex edges: " + sv.getOutEdges().size());
      for (SubgraphEdge<DoubleWritable, LongWritable, LongWritable> se : sv.getOutEdges()) {
        //System.out.println("Subgraph ID  : " + subgraph.getId().getSubgraphId() +"\t its vertex : " + sv.getId() + " has edge pointing to " + se.getSinkVertexId()+"\n");

        if (!vertices.containsKey(se.getSinkVertexId())) {
          //System.out.println("Parent subgraph " + subgraph.getId().getSubgraphId() +"does not contain the vertex id  : " + se.getSinkVertexId());
          remoteVertexIds.add(se.getSinkVertexId());
        }
      }
    }

    LOG.info("Partition,Subgraph,Vertices,RemoteVertices,Edges:" + subgraph.getPartitionId() + "," + subgraph.getSubgraphId() + "," + subgraph.getSubgraphVertices().getLocalVertices().size() + "," + remoteVertexIds.size() + "," + edgeCount);

    subgraph.getId().write(dataOutput);
//    LOG.info("Test, Sender subgraphID is : " + subgraph.getId());
    dataOutput.writeInt(remoteVertexIds.size());
//    LOG.info("Test, Sender number of remote vertices are  : " + remoteVertexIds.size());
//    LOG.info("Test, Number of edges: " + subgraph.getNumEdges());
    //LOG.info("Test, Number of  vertices are " + vertices.size());

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
    //LOG.info("Test 2, Free memory: " + freeMemoryMB());

    // LOG.info("Test, All messages sent");
    sendToNeighbors(subgraph, bw);
  }
}
