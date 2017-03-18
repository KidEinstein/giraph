package in.dream_lab.goffish.giraph;

import in.dream_lab.goffish.api.RemoteSubgraphVertex;
import in.dream_lab.goffish.api.Subgraph;
import in.dream_lab.goffish.api.SubgraphEdge;
import in.dream_lab.goffish.api.SubgraphVertex;
import org.apache.giraph.graph.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <SV> Subgraph Value type
 */
public class DefaultSubgraph<SV extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, EI extends WritableComparable, S extends WritableComparable
    >
    extends DefaultVertex<SubgraphId<S>, SubgraphVertices<SV, V, E, I, EI, S>, E> implements Subgraph<SV, V, E, I, EI, S> {

  public void setRemoteVertices(HashMap<I, RemoteSubgraphVertex<V, E, I, EI, S>> remoteVertices) {
    SubgraphVertices<SV, V, E, I, EI, S> subgraphVertices = getValue();
    subgraphVertices.setRemoteVertices(remoteVertices);
  }

  public Iterable<RemoteSubgraphVertex<V, E, I, EI, S>> getRemoteVertices() {
    SubgraphVertices<SV, V, E, I, EI, S> subgraphVertices = getValue();
    return subgraphVertices.getRemoteVertices().values();
  }

  @Override
  public SubgraphEdge<E, I, EI> getEdgeById(EI edgeId) {
    return null;
  }

  @Override
  public void setSubgraphValue(SV value) {
    getSubgraphVertices().setSubgraphValue(value);
  }

  @Override
  public SV getSubgraphValue() {
    return getSubgraphVertices().getSubgraphValue();
  }


  public SubgraphVertices<SV, V, E, I, EI, S> getSubgraphVertices() {
    return getValue();
  }

  @Override
  public SubgraphVertex<V, E, I, EI> getVertexById(I vertexId) {
    return getSubgraphVertices().getVertexById(vertexId);
  }

  @Override
  public S getSubgraphId() {
    return getId().getSubgraphId();
  }

  @Override
  public long getVertexCount() {
    return getSubgraphVertices().getNumVertices() + getSubgraphVertices().getNumRemoteVertices();
  }

  @Override
  public long getLocalVertexCount() {
    return getSubgraphVertices().getNumVertices();
  }

  @Override
  public Iterable<SubgraphVertex<V, E, I, EI>> getVertices() {
    return getSubgraphVertices().getVertices();
  }

  @Override
  public Iterable<SubgraphVertex<V, E, I, EI>> getLocalVertices() {
    return getSubgraphVertices().getLocalVertices().values();
  }

  public int getPartitionId() {
    return getId().getPartitionId();
  }

  public Iterable<SubgraphEdge<E, I, EI>> getOutEdges() {
    return new Iterable<SubgraphEdge<E, I, EI>>() {
      @Override
      public Iterator<SubgraphEdge<E, I, EI>> iterator() {
        return new EdgeIterator();
      }
    };
  }

  private final class EdgeIterator implements Iterator<SubgraphEdge<E, I, EI>> {
    Iterator<SubgraphVertex<V, E, I, EI>> vertexMapIterator;
    Iterator<SubgraphEdge<E, I, EI>> edgeIterator;

    public EdgeIterator() {
      vertexMapIterator = getVertices().iterator();
      SubgraphVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
      edgeIterator = nextVertex.getOutEdges().iterator();
    }

    @Override
    public boolean hasNext() {
      if (edgeIterator.hasNext()) {
        return true;
      } else {
        while (vertexMapIterator.hasNext()) {
          SubgraphVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return true;
          }
        }
      }
      return false;
    }

    public SubgraphEdge<E, I, EI> next() {
      if (edgeIterator.hasNext()) {
        return edgeIterator.next();
      } else {
        while (vertexMapIterator.hasNext()) {
          SubgraphVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return edgeIterator.next();
          }
        }
      }
      return null;
    }

    // TODO: Raise exception on call to remove
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }



}