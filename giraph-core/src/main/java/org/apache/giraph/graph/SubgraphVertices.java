package org.apache.giraph.graph;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
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
        I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable> implements Writable {
    private long numVertices;

    private LinkedList<SubgraphVertex<S, I, V, E, EI>> vertices;

    public long getNumVertices() {
        return numVertices;
    }

    public LinkedList<SubgraphVertex<S, I, V, E, EI>> getVertices() {
        return vertices;
    }

    public void initialize(LinkedList<SubgraphVertex<S, I, V, E, EI>> vertices) {
        this.vertices = vertices;
    }

    public Iterable<SubgraphEdge<I, E, EI>> getEdges() {
        // TODO: Loop through all vertices and return edges
        return null;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(vertices.size());
        for (SubgraphVertex<S, I, V, E, EI> vertex : getVertices()) {
            vertex.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int numVertices = dataInput.readInt();
        vertices = Lists.newLinkedList();
        for (int i = 0; i < numVertices; i++) {
            SubgraphVertex<S, I, V, E, EI> subgraphVertex = new SubgraphVertex<S, I, V, E, EI>() {};
            subgraphVertex.readFields(dataInput);
            vertices.add(subgraphVertex);
        }
    }
}
