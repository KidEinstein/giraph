package org.apache.giraph.graph;

import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
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

    public SubgraphVertices() {
        System.out.println("Calling subgraph vertices constructor");
        try {
            System.out.println("Inside try");
            throw new Exception("Calling constructor");
        } catch(Exception e) {
            System.out.println("Inside catch");
            e.printStackTrace(System.out);
            e.printStackTrace();
        }
    }

    public long getNumVertices() {
        return vertices.size();
    }

    public LinkedList<SubgraphVertex<S, I, V, E, EI>> getVertices() {
        return vertices;
    }

    public void initialize(LinkedList<SubgraphVertex<S, I, V, E, EI>> vertices) {
        // TODO: Log usage
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
        try {
            System.out.println("Read field for subgraph vertices without conf");
            throw new Exception("Calling readFields without conf");
        } catch(Exception e) {
            System.out.println("Inside catch");
            e.printStackTrace(System.out);
            e.printStackTrace();
        }
        int numVertices = dataInput.readInt();
        vertices = Lists.newLinkedList();
        for (int i = 0; i < numVertices; i++) {
            SubgraphVertex<S, I, V, E, EI> subgraphVertex = new DefaultSubgraphVertex<S, I, V, E, EI>();
            subgraphVertex.readFields(dataInput);
            vertices.add(subgraphVertex);
        }
    }

    public void readFields(ImmutableClassesGiraphConfiguration conf, DataInput dataInput) throws IOException {
        try {
            System.out.println("Read field for subgraph vertices with conf");
            throw new Exception();
        } catch(Exception e) {
            System.out.println("Calling readFields with conf");
            e.printStackTrace(System.out);
            e.printStackTrace();
        }
        int numVertices = dataInput.readInt();
        vertices = Lists.newLinkedList();
        for (int i = 0; i < numVertices; i++) {
            SubgraphVertex<S, I, V, E, EI> subgraphVertex = new DefaultSubgraphVertex<S, I, V, E, EI>();
            subgraphVertex.readFields(conf, dataInput);
            vertices.add(subgraphVertex);
        }
    }

}
