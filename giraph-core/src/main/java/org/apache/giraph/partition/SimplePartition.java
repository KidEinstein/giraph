/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import io.netty.buffer.ByteBufOutputStream;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

/**
 * A simple map-based container that stores vertices.  Vertex ids will map to
 * exactly one partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@ThreadSafe
@SuppressWarnings("rawtypes")
public class SimplePartition<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends BasicPartition<I, V, E> {
  /** Vertex map for this range (keyed by index) */
  private ConcurrentMap<I, Vertex<I, V, E>> vertexMap;
    /** Class logger */
  private static final Logger LOG =
            Logger.getLogger(SimplePartition.class);

  /**
   * Constructor for reflection.
   */
  public SimplePartition() { }

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    super.initialize(partitionId, progressable);
    vertexMap = Maps.newConcurrentMap();
  }

  @Override
  public Vertex<I, V, E> getVertex(I vertexIndex) {
    return vertexMap.get(vertexIndex);
  }

  @Override
  public Vertex<I, V, E> putVertex(Vertex<I, V, E> vertex) {
    return vertexMap.put(vertex.getId(), vertex);
  }

  @Override
  public Vertex<I, V, E> removeVertex(I vertexIndex) {
    return vertexMap.remove(vertexIndex);
  }

  @Override
  public boolean putOrCombine(Vertex<I, V, E> vertex) {
    Vertex<I, V, E> originalVertex = vertexMap.get(vertex.getId());
    if (originalVertex == null) {
      originalVertex =
          vertexMap.putIfAbsent(vertex.getId(), vertex);
      if (originalVertex == null) {
        return true;
      }
    }

    synchronized (originalVertex) {
      // Combine the vertex values
      getVertexValueCombiner().combine(
          originalVertex.getValue(), vertex.getValue());

      // Add the edges to the representative vertex
      for (Edge<I, E> edge : vertex.getEdges()) {
        originalVertex.addEdge(edge);
      }
    }

    return false;
  }

  @Override
  public void addPartition(Partition<I, V, E> partition) {
    for (Vertex<I, V, E> vertex : partition) {
      putOrCombine(vertex);
    }
  }

  @Override
  public long getVertexCount() {
    return vertexMap.size();
  }

  @Override
  public long getEdgeCount() {
    long edges = 0;
    for (Vertex<I, V, E> vertex : vertexMap.values()) {
      edges += vertex.getNumEdges();
    }
    return edges;
  }

  @Override
  public void saveVertex(Vertex<I, V, E> vertex) {
    // No-op, vertices are stored as Java objects in this partition
  }

  @Override
  public String toString() {
    return "(id=" + getId() + ",V=" + vertexMap.size() + ")";
  }

  @Override
  public void readFields(DataInput input) throws IOException {

      super.readFields(input);

      int pid= getId();

      int vertices = input.readInt();

      //FIXME: for lazy loading we need to load only sgid --get the sg object-- read the value
      for (int i = 0; i < vertices; ++i) {
          progress();

          long vid=input.readInt();

          Vertex vertex=getVertex(vid);

          vertex.getValue().readFields(input);

    }


//    long startTime=System.currentTimeMillis();
//    super.readFields(input);
//    vertexMap = Maps.newConcurrentMap();
//    int vertices = input.readInt();
////    System.out.println(" read fields is being called !! with vertices : "+ vertices);
//
//    for (int i = 0; i < vertices; ++i) {
//      progress();
//      Vertex<I, V, E> vertex =
//          WritableUtils.readVertexFromDataInput(input, getConf());
//      if (vertexMap.put(vertex.getId(), vertex) != null) {
//        throw new IllegalStateException(
//            "readFields: " + this +
//            " already has same id " + vertex);
//      }
//    }
//    LOG.debug("TEST,SimplePartition.read,vertexLoop,"+vertices+",took,"+(System.currentTimeMillis()-startTime));
  }

  @Override
  public void write(DataOutput output) throws IOException {
    long startTime=System.currentTimeMillis();
    super.write(output);
    output.writeInt(vertexMap.size());
    for (Vertex<I, V, E> vertex : vertexMap.values()) {
      progress();
      WritableUtils.writeVertexToDataOutput(output, vertex, getConf());
    }
    ByteBufOutputStream b= (ByteBufOutputStream) output;

    LOG.debug("TEST,SimplePartition.write,vertexLoop,"+vertexMap.size()+",took,"+(System.currentTimeMillis()-startTime)+",size,"+b.writtenBytes()+",id,"+getId());
//      LOG.debug();
  }

  @Override
  public Iterator<Vertex<I, V, E>> iterator() {
    return vertexMap.values().iterator();
  }
}
