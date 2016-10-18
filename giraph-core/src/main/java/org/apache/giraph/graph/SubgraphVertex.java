package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 */
public class SubgraphVertex<S extends WritableComparable, I extends WritableComparable,
        V extends Writable, E extends Writable> implements WritableComparable {
    private SubgraphId<S> subgraphId;
    private I id;
    private V value;
    private Iterable<Edge<I, E>> outEdges;

    public Iterable<Edge<I, E>> getOutEdges() {
        return outEdges;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    public I getId() {
        return id;
    }

    public V getValue() {
        return value;
    }

    public SubgraphId<S> getSubgraphId() {
        return subgraphId;
    }

    public boolean isRemote() {
        return false;
    }

    public void initialize(SubgraphId<S> subgraphId, I vertexId, V value, Iterable<Edge<I, E>> edges) {
        this.subgraphId = subgraphId;
        this.id = vertexId;
        this.value = value;
        this.outEdges = edges;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        subgraphId.write(dataOutput);
        id.write(dataOutput);
        value.write(dataOutput);
        // TODO: Write edges
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        subgraphId = new SubgraphId<>();
        subgraphId.readFields(dataInput);
        id = (I) new LongWritable();
        id.readFields(dataInput);

        value = (V) new DoubleWritable();
        value.readFields(dataInput);

        // TODO: Read edges
    }

    //TODO: Do we need hashcode



}
