package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 27/09/16.
 * @param <S> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge data
 * @param <SV> Subgraph Value type
 *
 */
public class DefaultSubgraph<S extends WritableComparable,
        I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable>
        extends DefaultVertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI> , E> implements Subgraph<S, I, V, E, SV, EI> {

    private Iterable<SubgraphVertex<S, I, Writable, E, EI>> remoteVertices;

    @Override
    public Iterable<Edge<SubgraphId<S>, E>> getRemoteEdges() {
        return getEdges();
    }

    @Override
    public Iterable<SubgraphVertex<S, I, Writable, E, EI>> getRemoteVertices() {
        return remoteVertices;
    }

    @Override
    public SubgraphVertices<S, I, V, E, SV, EI> getSubgraphVertices() {
        return getValue();
    }

}
