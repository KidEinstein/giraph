package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    private Iterable<SubgraphVertex<S, I, V, E>> vertices;

    public long getNumVertices() {
        return numVertices;
    }

    public Iterable<SubgraphVertex<S, I, V, E>> getVertices() {
        return vertices;
    }

    public void initialize(Iterable<SubgraphVertex<S, I, V, E>> vertices) {
        this.vertices = vertices;
    }

    public Iterable<Edge<I, E>> getEdges() {
        // TODO: Loop through all vertices and return edges
        return null;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
