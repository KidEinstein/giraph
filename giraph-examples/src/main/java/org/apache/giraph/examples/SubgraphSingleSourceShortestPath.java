package org.apache.giraph.examples;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by anirudh on 25/01/17.
 */
public class SubgraphSingleSourceShortestPath extends SubgraphComputation<LongWritable,
    LongWritable, LongWritable, NullWritable, BytesWritable, LongWritable, NullWritable> {

  @Override
  public void compute(Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable, NullWritable> subgraph, Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
    SubgraphVertices<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable, NullWritable> subgraphVertices = subgraph.getSubgraphVertices();
    HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> vertices = subgraphVertices.getVertices();
    PriorityQueue<DistanceVertex> localUpdateQueue = new PriorityQueue<>();
    // Initialization step
    if (getSuperstep() == 3) {
      // Initializing distance to max distance
      for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex : vertices.values()) {
        vertex.setValue(new LongWritable(Long.MAX_VALUE));
      }
      // Setting distance to 0 for source vertex
      long sourceId = getConf().getSubgraphSourceVertex();
      LongWritable sourceIdLongWritable = new LongWritable(sourceId);
      if (vertices.containsKey(sourceIdLongWritable)) {
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> sourceVertex = vertices.get(sourceIdLongWritable);
        sourceVertex.setValue(new LongWritable(0));
        localUpdateQueue.add(new DistanceVertex(sourceVertex, 0));
      }
    }
    // Update steps
    else {
      for (SubgraphMessage<LongWritable, BytesWritable> subgraphMessage : subgraphMessages) {
        BytesWritable subgraphMessageValue = subgraphMessage.getMessage();

        ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(subgraphMessageValue.getBytes());
        int numVertices = dataInput.readInt();
        for (int i = 0; i < numVertices; i++) {
          long sinkVertex = dataInput.readLong();
          long sinkDistance = dataInput.readLong();
          SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> currentVertex = vertices.get(new LongWritable(sinkVertex));
          long distance = currentVertex.getValue().get();
          if (sinkDistance < distance) {
            currentVertex.setValue(new LongWritable(sinkDistance));
            localUpdateQueue.add(new DistanceVertex(currentVertex, sinkDistance));
          }
        }
      }

      // Dijkstra's
      HashMap<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> remoteVertexUpdates = new HashMap<>();
      DistanceVertex currentDistanceVertex;
      while ((currentDistanceVertex = localUpdateQueue.poll()) != null) {

        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> updatedVertex = currentDistanceVertex.vertex;
        for (SubgraphEdge<LongWritable, NullWritable, NullWritable> edge : updatedVertex.getOutEdges()) {
          long newDistance = currentDistanceVertex.distance + edge.getValue();
          LongWritable newDistanceLongWritable = new LongWritable(newDistance);
          LongWritable sinkVertexId = edge.getSinkVertexId();
          HashMap<LongWritable, RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> remoteVertices = subgraph.getRemoteVertices();
          if (!remoteVertices.containsKey(sinkVertexId)) {
            // Local neighboring vertex
            SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> neighborVertex = vertices.get(sinkVertexId);
            if (neighborVertex.getValue().compareTo(newDistanceLongWritable) > 0) {
              neighborVertex.setValue(newDistanceLongWritable);
              DistanceVertex distanceVertex = new DistanceVertex(neighborVertex, newDistance);
              if (!localUpdateQueue.contains(distanceVertex)) {
                localUpdateQueue.add(distanceVertex);
              } else {
                // Works because of overriding equals to only compare the vertex object and not the distance
                localUpdateQueue.remove(distanceVertex);
                localUpdateQueue.add(distanceVertex);
              }
            }
          } else {
            // Remote neighboring vertex
            RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> remoteSubgraphVertex = remoteVertices.get(sinkVertexId);
            if (!remoteVertexUpdates.containsKey(remoteSubgraphVertex)) {
              // Every subsequent iteration of the while loop would have a greater distance for the remote
              remoteVertexUpdates.put(remoteSubgraphVertex, newDistance);
            } else {
              Long distance = remoteVertexUpdates.get(remoteSubgraphVertex);
              if (distance > newDistance) {
                remoteVertexUpdates.put(remoteSubgraphVertex, newDistance);
              }
            }
          }
        }

      }


     ;
      for (Map.Entry<RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>, Long> entries : remoteVertexUpdates.entrySet()) {
        ExtendedByteArrayDataOutput dataOutput = new ExtendedByteArrayDataOutput();
        RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> remoteSubgraphVertex = entries.getKey();
        dataOutput.writeLong(remoteSubgraphVertex.getId().get());
        dataOutput.writeLong(entries.getValue());
        BytesWritable message = new BytesWritable((dataOutput.getByteArray()));
        sendMessage(remoteSubgraphVertex.getSubgraphId(), message);
      }


    }
  }

  private static class DistanceVertex implements Comparable<DistanceVertex> {
    public long distance;
    public SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex;

    public DistanceVertex(SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex_, long distance_) {
      vertex = vertex_;
      distance = distance_;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DistanceVertex) {
        DistanceVertex other = (DistanceVertex) obj;
        return vertex.equals(other.vertex)
      }
      return super.equals(obj);
    }

    @Override
    public int compareTo(DistanceVertex o) {
      return (int) (distance - o.distance);
    }
  }
}
