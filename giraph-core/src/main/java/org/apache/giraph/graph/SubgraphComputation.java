package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 * @param <S> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge data
 * @param <M> Message type
 * @param <SV> Subgraph Value type
 */


// S subgraph value type ----- SV now
// V vertex object type   -- V is the vertex value
// E edge value type    -- E is the edge value
// M msg object type    -- M is the message value type
// I vertex id      --- I is the vertex id here
// J edge id        --
// K subgraph id


public abstract class SubgraphComputation<S extends WritableComparable,
        I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends BasicComputation<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E, M> {

    public abstract void compute(Subgraph<S, I, V, E, SV, EI> subgraph, Iterable<M> messages) throws IOException;

    // TODO: Change to the goffishv3 API

    // TODO: Take care of state changes for the subgraph passed

    // TODO: Have separate remote vertices interface

    @Override
    public void compute(Vertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> vertex, Iterable<M> messages) throws IOException {
        compute((Subgraph)vertex, messages);
    }
}
