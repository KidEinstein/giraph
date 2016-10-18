package org.apache.giraph.graph;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 * @param <S> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */

public class SubgraphVertices<S extends WritableComparable,
        I extends WritableComparable, V extends Writable, E extends Writable> implements Writable {
    private long numVertices;

    private LinkedList<SubgraphVertex<S, I, V, E>> vertices;

    public long getNumVertices() {
        return numVertices;
    }

    public LinkedList<SubgraphVertex<S, I, V, E>> getVertices() {
        return vertices;
    }

    public void initialize(LinkedList<SubgraphVertex<S, I, V, E>> vertices) {
        this.vertices = vertices;
    }

    public Iterable<Edge<I, E>> getEdges() {
        // TODO: Loop through all vertices and return edges
        return null;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(vertices.size());
        for (SubgraphVertex<S, I, V, E> vertex : getVertices()) {
            vertex.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int numVertices = dataInput.readInt();
        vertices = Lists.newLinkedList();
        for (int i = 0; i < numVertices; i++) {
            SubgraphVertex subgraphVertex = new SubgraphVertex();
            subgraphVertex.readFields(dataInput);
            vertices.add(subgraphVertex);
        }
    }
}
