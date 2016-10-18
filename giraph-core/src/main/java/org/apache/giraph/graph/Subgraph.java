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
 */
public class Subgraph<S extends WritableComparable,
        I extends WritableComparable, V extends WritableComparable, E extends Writable>
        extends DefaultVertex<SubgraphId<S>, SubgraphVertices<S, I, V, E> , E> {

    private Iterable<SubgraphVertex<S, I, Writable, E>> remoteVertices;

    public Iterable<Edge<SubgraphId<S>, E>> getRemoteEdges() {
        return getEdges();
    }

    public Iterable<SubgraphVertex<S, I, Writable, E>> getRemoteVertices() {
        return remoteVertices;
    }

    public SubgraphVertices getSubgraphVertices() {
        return getValue();
    }

}
