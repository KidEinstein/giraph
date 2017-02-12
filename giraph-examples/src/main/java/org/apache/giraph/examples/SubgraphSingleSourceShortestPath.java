package org.apache.giraph.examples;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Created by anirudh on 25/01/17.
 */
public class SubgraphSingleSourceShortestPath extends SubgraphComputation<LongWritable,
    LongWritable, LongWritable, NullWritable, BytesWritable, LongWritable, NullWritable> {
  public static final Logger LOG = Logger.getLogger(SubgraphSingleSourceShortestPath.class);

  @Override
  public void compute(Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable, NullWritable> subgraph, Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
//    LOG.info("Super step: " + getSuperstep() + "SubgraphID: " + subgraph.getId().getSubgraphId());
//    state = subgraph.getId().getSubgraphId().get();
    SubgraphVertices<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable, NullWritable> subgraphVertices = subgraph.getSubgraphVertices();
    HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> vertices = subgraphVertices.getVertices();
//    long startTime = System.currentTimeMillis();
    HashMap<LongWritable, DistanceVertex> localUpdateMap = new HashMap<>();
    int messageReceivedCount = 0;
    int messageReceivedUnpackedCount = 0;
    int insertionCount = 0;
    // Initialization step
    if (getSuperstep() == 0) {
      LOG.info("Partition,Subgraph,Vertices:" + subgraph.getId().getPartitionId() + "," + subgraph.getId().getSubgraphId() + "," + subgraph.getSubgraphVertices().getVertices().size());
      // Initializing distance to max distance
      for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex : vertices.values()) {
        vertex.setValue(new LongWritable(Long.MAX_VALUE));
      }
      // Setting distance to 0 for source vertex
      long sourceId = getConf().getSubgraphSourceVertex();
      LongWritable sourceIdLongWritable = new LongWritable(sourceId);
      if (vertices.containsKey(sourceIdLongWritable)) {
        LOG.info("Found source vertex, source id " + sourceId + " subgraph ID " + subgraph.getId().getSubgraphId() + " partition id " + subgraph.getId().getPartitionId());
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> sourceVertex = vertices.get(sourceIdLongWritable);
        sourceVertex.setValue(new LongWritable(0));
        DistanceVertex distanceVertex = new DistanceVertex(sourceVertex, 0);
        localUpdateMap.put(sourceVertex.getId(), distanceVertex);
      }
    }
    // Update steps
    else {
      for (SubgraphMessage<LongWritable, BytesWritable> subgraphMessage : subgraphMessages) {
        messageReceivedCount++;
        BytesWritable subgraphMessageValue = subgraphMessage.getMessage();
        ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(subgraphMessageValue.getBytes());
        while (!dataInput.endOfInput()) {
          messageReceivedUnpackedCount++;
          long sinkVertex = dataInput.readLong();
          if (sinkVertex == -1) {
            break;
          }
          long sinkDistance = dataInput.readLong();
          //LOG.info("Test, Sink vertex received: " + sinkVertex);
          SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> currentVertex = vertices.get(new LongWritable(sinkVertex));
          //LOG.info("Test, Current vertex object: " + currentVertex);

          //LOG.info("Test, Current vertex: " + currentVertex.getId());
          long distance = currentVertex.getValue().get();
          if (sinkDistance < distance) {
            insertionCount++;
            currentVertex.setValue(new LongWritable(sinkDistance));
            localUpdateMap.put(currentVertex.getId(), new DistanceVertex(currentVertex, sinkDistance));
          }
        }
      }

    }
//    LOG.info("Message processing time: " + (System.currentTimeMillis() - startTime));
    long startTime = System.currentTimeMillis();
    HashMap<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> remoteVertexUpdates = aStar(localUpdateMap, vertices, subgraph.getSubgraphVertices().getRemoteVertices(), subgraph.getId());
//    LOG.info("Number of vertices processed in queue: " + count);
    long dijkstraTime = System.currentTimeMillis() - startTime;
    long startTime2 = System.currentTimeMillis();
    int messageCount = packAndSendMessages(remoteVertexUpdates);
    LOG.info("MESSAGE STATS-PartitionID,SubgraphID,Superstep,messages," + subgraph.getId().getPartitionId() + "," + subgraph.getId().getSubgraphId() + "," + getSuperstep() + "," + messageCount);
    LOG.info("Partition,Superstep,Subgraph,MessageCount,MessageUnpackedCount,LocalMapSize,InsertionCount,DijstraTime,packandsendTime:" +
        subgraph.getId().getPartitionId() + "," + getSuperstep() + "," + subgraph.getId().getSubgraphId() + ","
        + messageReceivedCount + "," + messageReceivedUnpackedCount + "," + localUpdateMap.size() + "," + insertionCount
        + "," + dijkstraTime + "," + (System.currentTimeMillis() - startTime2));
    subgraph.voteToHalt();

  }

  private HashMap<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long>
  aStar(HashMap<LongWritable, DistanceVertex> localUpdateMap,
        HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> vertices,
        HashMap<LongWritable, RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> remoteVertices, SubgraphId subgraphId) {
    // Dijkstra's
    HashMap<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> remoteVertexUpdates = new HashMap<>();
    DistanceVertex currentDistanceVertex;
    int count = 0, addedNew = 0, replacedOld = 0, remoteAddNew = 0, remoteReplaced = 0;
    PriorityQueue<DistanceVertex> localUpdateQueue = new PriorityQueue<>(localUpdateMap.values());
    while ((currentDistanceVertex = localUpdateQueue.poll()) != null) {
      count++;
      SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> updatedVertex = currentDistanceVertex.vertex;
      for (SubgraphEdge<LongWritable, NullWritable, NullWritable> edge : updatedVertex.getOutEdges()) {
        // TODO: Change 1 to edge.getValue()
        long newDistance = currentDistanceVertex.distance + 1;
        LongWritable sinkVertexId = edge.getSinkVertexId();
        if (!remoteVertices.containsKey(sinkVertexId)) {
          // Local neighboring vertex
          SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> neighborVertex = vertices.get(sinkVertexId);
          if (neighborVertex.getValue().get() > newDistance) {
            neighborVertex.setValue(new LongWritable(newDistance));
            DistanceVertex distanceVertex = new DistanceVertex(neighborVertex, newDistance);
            if (!localUpdateMap.containsKey(neighborVertex.getId())) {
              localUpdateMap.put(neighborVertex.getId(), distanceVertex);
              localUpdateQueue.add(distanceVertex);
              addedNew++;
            } else {
              // Works because of overriding equals to only compare the vertex object and not the distance
              localUpdateQueue.remove(distanceVertex);
              localUpdateQueue.add(distanceVertex);
              replacedOld++;
            }
          }
        } else {
          // Remote neighboring vertex
          RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> remoteSubgraphVertex = remoteVertices.get(sinkVertexId);
          if (!remoteVertexUpdates.containsKey(remoteSubgraphVertex)) {
            // Every subsequent iteration of the while loop would have a greater distance for the remote
            remoteVertexUpdates.put(remoteSubgraphVertex, newDistance);
            remoteAddNew++;
          } else {
            Long distance = remoteVertexUpdates.get(remoteSubgraphVertex);
            if (distance > newDistance) {
              remoteVertexUpdates.put(remoteSubgraphVertex, newDistance);
              remoteReplaced++;
            }
          }
        }
      }
    }
    LOG.info("Superstep,Parition,SubgraphId,pollCount,addedNew,replacedOld,remoteAddNew,remoteReplaced:" +
    getSuperstep() + "," + subgraphId.getPartitionId() + "," + subgraphId.getSubgraphId() + "," + count + "," +
    addedNew + "," + replacedOld + "," + remoteAddNew + "," + remoteReplaced);
    return remoteVertexUpdates;
  }

  int packAndSendMessages(HashMap<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> remoteVertexUpdates) throws IOException {
    HashMap<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> messagesMap = new HashMap<>();
    for (Map.Entry<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> entry : remoteVertexUpdates.entrySet()) {
      RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> remoteSubgraphVertex = entry.getKey();
      ExtendedByteArrayDataOutput dataOutput;
      if (!messagesMap.containsKey(remoteSubgraphVertex.getSubgraphId())) {
        dataOutput = new ExtendedByteArrayDataOutput();
        messagesMap.put(remoteSubgraphVertex.getSubgraphId(), dataOutput);
      } else {
        dataOutput = messagesMap.get(remoteSubgraphVertex.getSubgraphId());
      }
      dataOutput.writeLong(remoteSubgraphVertex.getId().get());
      dataOutput.writeLong(entry.getValue());
      // LOG.info("SubgraphID" + remoteSubgraphVertex.getSubgraphId() + " Sending vertex id " + remoteSubgraphVertex.getId().get() + " distance "+ entry.getValue());
    }
    int messageCount = 0;
    for (Map.Entry<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> entry : messagesMap.entrySet()) {
      ExtendedByteArrayDataOutput dataOutput = entry.getValue();
      dataOutput.writeLong(-1);
      sendMessage(entry.getKey(), new BytesWritable(dataOutput.getByteArray()));
      messageCount++;
    }
    return messageCount;
  }

  private static class DistanceVertex implements Comparable<DistanceVertex> {
    long distance;
    SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex;

    DistanceVertex(SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex_, long distance_) {
      vertex = vertex_;
      distance = distance_;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DistanceVertex) {
        DistanceVertex other = (DistanceVertex) obj;
        return vertex.equals(other.vertex);
      }
      return super.equals(obj);
    }

    @Override
    public int compareTo(DistanceVertex o) {
      return (int) (distance - o.distance);
    }
  }
}
