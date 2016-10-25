package org.apache.giraph.graph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.weakref.jmx.com.google.common.reflect.Reflection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 */
public class SubgraphVertex<S extends WritableComparable, I extends WritableComparable,
        V extends Writable, E extends Writable, EI extends WritableComparable> implements WritableComparable {
    private SubgraphId<S> subgraphId;
    private I id;
    private V value;
    private Iterable<SubgraphEdge<I, E, EI>> outEdges;

    public Iterable<SubgraphEdge<I, E, EI>> getOutEdges() {
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


    // TODO: Move to remote vertex
    public SubgraphId<S> getSubgraphId() {
        return subgraphId;
    }

    public boolean isRemote() {
        return false;
    }

    public void initialize(SubgraphId<S> subgraphId, I vertexId, V value, Iterable<SubgraphEdge<I, E, EI>> edges) {
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
        // Getting the subgraph vertex internals
        subgraphId = new SubgraphId<S>() {};
        subgraphId.readFields(dataInput);

        Class<I> idClass = (Class<I>) GiraphConstants.SUBGRAPH_VERTEX_ID_CLASS.getDefaultClass();
        id = ReflectionUtils.newInstance(idClass);

        Class<V> valueClass = (Class<V>) GiraphConstants.SUBGRAPH_VERTEX_VALUE_CLASS.getDefaultClass();
        value = ReflectionUtils.newInstance(valueClass);

        id.readFields(dataInput);
        value.readFields(dataInput);

        // TODO: Read edges
    }

    //TODO: Do we need hashcode



}
