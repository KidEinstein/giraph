package org.apache.giraph.graph;

import org.apache.giraph.comm.messages.SubgraphMessage;
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
// J edge id        -- EI
// K subgraph id  ---- S


public abstract class SubgraphComputation<S extends WritableComparable,
        I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends BasicComputation<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E, SubgraphMessage<S, M>> {

    public abstract void compute(Subgraph<S, I, V, E, SV, EI> subgraph, Iterable<SubgraphMessage<S, M>> messages) throws IOException;

    // TODO: Take care of state changes for the subgraph passed

    public void compute(Vertex<SubgraphId<S>, SubgraphVertices<S, I, V, E, SV, EI>, E> vertex, Iterable<SubgraphMessage<S, M>> messages) throws IOException {
        compute((Subgraph)vertex, messages);
    }

    public void sendMessageToAllNeighboringSubgraphs(Subgraph<S, I, V, E, SV, EI> subgraph, M message) {
        WritableComparable subgraphId = subgraph.getId().getSubgraphId();
        SubgraphMessage sm = new SubgraphMessage(subgraphId, message);
        super.sendMessageToAllEdges(subgraph, sm);
    }

    public void sendMessage(SubgraphId<S> subgraphId, M message) {
        SubgraphMessage sm = new SubgraphMessage(subgraphId.getSubgraphId(), message);
        sendMessage(subgraphId, sm);
    }
}
