package in.dream_lab.goffish.giraph.formats;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.*;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 * Created by anirudh on 15/05/17.
 */
public abstract class AdjacencyListFlatTextSubgraphInputFormat<S extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> extends
    FlatTextSubgraphInputFormat<SubgraphId<K>, SubgraphVertices<S,V,E,I,J,K>, NullWritable> {
  /** Delimiter for split */
  public static final String LINE_TOKENIZE_VALUE = "adj.list.input.delimiter";
  /** Default delimiter for split */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  /**
   * Utility for doing any cleaning of each line before it is tokenized.
   */
  public interface LineSanitizer {
    /**
     * Clean string s before attempting to tokenize it.
     *
     * @param s String to be cleaned.
     * @return Sanitized string.
     */
    String sanitize(String s);
  }

  @Override
  public abstract AdjacencyListFlatTextSubgraphReader createVertexReader(
      InputSplit split, TaskAttemptContext context);

  /**
   * Vertex reader associated with {@link AdjacencyListTextVertexInputFormat}.
   */
  protected abstract class AdjacencyListFlatTextSubgraphReader extends
      FlatTextVertexReaderFromEachLineProcessed<String[]> {
    private K subgraphId;
    private HashMap<I, IRemoteVertex<V, E, I, J, K>> remoteVertices;
    private HashSet<SubgraphId<K>> neighboringSubgraphs;

    /** Cached delimiter used for split */
    private String splitValue = null;
    /** Sanitizer from constructor. */
    private final LineSanitizer sanitizer;

    /**
     * Constructor without line sanitizer.
     */
    public AdjacencyListFlatTextSubgraphReader() {
      this(null);
    }

    /**
     * Constructor with line sanitizer.
     *
     * @param sanitizer Sanitizer to be used.
     */
    public AdjacencyListFlatTextSubgraphReader(LineSanitizer sanitizer) {
      this.sanitizer = sanitizer;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      splitValue =
          getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }




    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String sanitizedLine;
      if (sanitizer != null) {
        sanitizedLine = sanitizer.sanitize(line.toString());
      } else {
        sanitizedLine = line.toString();
      }
      String [] values = sanitizedLine.split(splitValue);
      return values;
    }


    protected I getVId(String[] values) throws IOException {
      return decodeId(values[1]);
    }

    /**
     * Decode the id for this line into an instance of its correct type.
     *
     * @param s Id of vertex from line
     * @return Vertex id
     */
    public abstract I decodeId(String s);

    public abstract K decodeSId(String s);

    public abstract int decodePId(String s);





    /**
     * Decode the value for this line into an instance of its correct type.
     *
     * @param s Value from line
     * @return Vertex value
     */
    public abstract V decodeValue(String s);

    @Override
    public SubgraphId<K> getSId(String[] line) {
      subgraphId = decodeSId(line[2]);
      return new SubgraphId<K>(decodeSId(line[2]), decodePId(line[0]));
    }

    @Override
    public IVertex readVertex(String[] line) throws IOException {
      DefaultSubgraphVertex subgraphVertex = new DefaultSubgraphVertex();
      subgraphVertex.initialize(getVId(line), NullWritable.get(), getVertexEdges(line));
      return subgraphVertex;
    }

    @Override
    public SubgraphVertices<S, V, E, I, J, K> getSubgraphVertices() throws IOException, InterruptedException {
      remoteVertices = new HashMap<>();
      neighboringSubgraphs = new HashSet<>();
      SubgraphVertices<S, V, E, I, J, K> subgraphVertices = new SubgraphVertices<>();
      HashMap<I, IVertex<V, E, I, J>> subgraphVerticesMap = new HashMap<>();
      // Read each vertex
      Text vertexLine = getRecordReader().getCurrentValue();
      String[] processedLine = preprocessLine(vertexLine);

      IVertex<V, E, I, J> subgraphVertex = readVertex(processedLine);
      subgraphVerticesMap.put(subgraphVertex.getVertexId(), subgraphVertex);
      while (getRecordReader().nextKeyValue()) {
        // take all info from each line

        // Read each vertex
        vertexLine = getRecordReader().getCurrentValue();
        processedLine = preprocessLine(vertexLine);

        subgraphVertex = readVertex(processedLine);
        subgraphVerticesMap.put(subgraphVertex.getVertexId(), subgraphVertex);
      }
      GiraphSubgraphConfiguration giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(getConf());
      subgraphVertices.setRemoteVertices(remoteVertices);
      subgraphVertices.initialize(subgraphVerticesMap);
      subgraphVertices.setSubgraphValue((S) giraphSubgraphConfiguration.createSubgraphValue());

      for (Map.Entry<I, IVertex<V, E, I, J>> entry : subgraphVertices.getLocalVertices().entrySet()) {
        System.out.println("V ID: " + entry.getKey() + ", V Value: " + entry.getValue());
      }
      for (Map.Entry<I, IRemoteVertex<V, E, I, J, K>> entry : subgraphVertices.getRemoteVertices().entrySet()) {
        System.out.println("RV ID: " + entry.getKey() + ", RV Value: " + entry.getValue());
      }
      return subgraphVertices;
    }

    @Override
    protected LinkedList<IEdge> getVertexEdges(String[] values) throws
        IOException {
      int i = 3;
      LinkedList<IEdge> edges = Lists.newLinkedList();
      while (i < values.length) {
        I vertexId = decodeId(values[i]);
        IEdge<E, I, J> e = createVertexEdge(vertexId);
        edges.add(e);
        K remoteSId = decodeSId(values[i + 1]);
        int remotePId = Integer.parseInt(values[i + 2]);
        System.out.println("SID: " + subgraphId + ",RSID:" + remoteSId + ",RPID:" + remotePId);
        if (!subgraphId.equals(remoteSId)) {
          neighboringSubgraphs.add(new SubgraphId<>(remoteSId, remotePId));
          DefaultRemoteSubgraphVertex<V, E, I, J, K> remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
          remoteSubgraphVertex.setSubgraphId(remoteSId);
          remoteSubgraphVertex.setId(vertexId);
          remoteVertices.put(e.getSinkVertexId(), remoteSubgraphVertex);
        }

        i += 3;
      }
      return edges;
    }

    /**
     * Decode an edge from the line into an instance of a correctly typed Edge
     *
     * @param vertexId The edge's id from the line
     * @return Edge with given target id and value
     */
    public IEdge createVertexEdge(I vertexId) {
      DefaultSubgraphEdge<I, NullWritable, NullWritable> subgraphEdge = new DefaultSubgraphEdge<>();
      subgraphEdge.initialize(NullWritable.get(), NullWritable.get(), vertexId);
      return subgraphEdge;
    }

    @Override
    protected Iterable<Edge<SubgraphId<K>, NullWritable>> getSubgraphNeighbors(String[] values) throws
        IOException {
      List<Edge<SubgraphId<K>, NullWritable>> edges = Lists.newLinkedList();
      for (SubgraphId<K> sId: neighboringSubgraphs){
        edges.add(EdgeFactory.create(sId, NullWritable.get()));
      }
      return edges;
    }

  }
}
