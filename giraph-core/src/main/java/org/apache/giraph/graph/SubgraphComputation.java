package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 * @param <S> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge data
 * @param <M> Message type
 */

public abstract class SubgraphComputation<S extends WritableComparable,
        I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable> extends BasicComputation<S, SubgraphVertices<S, I, V, E>, E, M> {

    public abstract void compute(Subgraph<S, I, V, E> subgraph, Iterable<M> messages) throws IOException;

    @Override
    public void compute(Vertex<S, SubgraphVertices<S, I, V, E>, E> vertex, Iterable<M> messages) throws IOException {
        throw new UnsupportedOperationException("Use the subgraph compute method instead");
    }

}
