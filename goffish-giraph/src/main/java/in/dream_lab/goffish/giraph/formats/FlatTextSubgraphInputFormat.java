package in.dream_lab.goffish.giraph.formats;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.*;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 * Created by anirudh on 15/05/17.
 */
public abstract class FlatTextSubgraphInputFormat<S extends Writable, V extends Writable,
    E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> extends VertexInputFormat<SubgraphId<K>, SubgraphVertices<S,V,E,I,J,K>, NullWritable> {

  protected GiraphSubgraphTextInputFormat subgraphTextInputFormat = new GiraphSubgraphTextInputFormat();

  @Override
  public void checkInputSpecs(Configuration conf) {

  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
    return subgraphTextInputFormat.getVertexSplits(context);
  }

  @Override
  public abstract FlatTextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException;

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex input. Easiest to ignore the key value separator and only use
   * key instead.
   * <p>
   * When reading a vertex from each line, extend
   * {@link TextSubgraphInputFormat.TextVertexReaderFromEachLine}. If you need to preprocess each line
   * first, then extend {@link TextSubgraphInputFormat.TextVertexReaderFromEachLineProcessed}. If you
   * need common exception handling while preprocessing, then extend
   * {@link TextSubgraphInputFormat.TextVertexReaderFromEachLineProcessedHandlingExceptions}.
   */
  protected abstract class FlatTextVertexReader extends VertexReader<SubgraphId<K>, SubgraphVertices<S,V,E,I,J,K>, NullWritable> {
    /**
     * Internal line record reader
     */
    private RecordReader<LongWritable, Text> lineRecordReader;
    /**
     * Context passed to initialize
     */
    private TaskAttemptContext context;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      this.context = context;
      lineRecordReader = createLineRecordReader(inputSplit, context);
      lineRecordReader.initialize(inputSplit, context);
    }

    /**
     * Create the line record reader. Override this to use a different
     * underlying record reader (useful for testing).
     *
     * @param inputSplit the split to read
     * @param context    the context passed to initialize
     * @return the record reader to be used
     * @throws IOException          exception that can be thrown during creation
     * @throws InterruptedException exception that can be thrown during creation
     */
    protected RecordReader<LongWritable, Text>
    createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      return subgraphTextInputFormat.createRecordReader(inputSplit, context);
    }

    @Override
    public void close() throws IOException {
      lineRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lineRecordReader.getProgress();
    }

    /**
     * Get the line record reader.
     *
     * @return Record reader to be used for reading.
     */
    protected RecordReader<LongWritable, Text> getRecordReader() {
      return lineRecordReader;
    }

    /**
     * Get the context.
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
   * Abstract class to be implemented by the user to read a vertex from each
   * text line after preprocessing it.
   *
   */
  protected abstract class FlatTextVertexReaderFromEachLineProcessed extends
      FlatTextVertexReader {

    private boolean initialized;

    private HashMap<K, SubgraphInput> inputSubgraphs;

    private Iterator<SubgraphInput> subgraphInputIterator;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      inputSubgraphs = new HashMap<>();
      initialized = false;
    }

    @Override
    public final boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue() || subgraphInputIterator.hasNext();
    }

    @Override
    public final Vertex<SubgraphId<K>, SubgraphVertices<S,V,E,I,J,K>, NullWritable> getCurrentVertex() throws IOException,
        InterruptedException {
      if (!initialized) {
        GiraphSubgraphConfiguration giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(getConf());
        do {
          Text line = getRecordReader().getCurrentValue();
          String[] processed = preprocessLine(line);
          SubgraphId<K> subgraphId = getSId(processed);

          K sid = subgraphId.getSubgraphId();

          SubgraphInput subgraphInput;

          if (inputSubgraphs.containsKey(sid)) {
            subgraphInput = inputSubgraphs.get(sid);
          } else {
            subgraphInput = createSubgraphInput(giraphSubgraphConfiguration, subgraphId);
            inputSubgraphs.put(sid, subgraphInput);
          }

          subgraphInput.addEntry(processed);
        } while (getRecordReader().nextKeyValue());

        subgraphInputIterator = inputSubgraphs.values().iterator();

        initialized = true;
      }

      Vertex subgraph = getConf().createVertex();

      return subgraphInputIterator.next().populateSubgraph(subgraph);
    }

    public abstract SubgraphInput createSubgraphInput(GiraphSubgraphConfiguration conf, SubgraphId<K> sid);

    public abstract SubgraphId<K> getSId(String[] line);

    /**
     * ,
     * Preprocess the line so other methods can easily read necessary
     * information for creating vertex.
     *
     * @param line the current line to be read
     * @return the preprocessed object
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract String[] preprocessLine(Text line) throws IOException;
  }

  public abstract class SubgraphInput {

    private SubgraphId<K> subgraphId;

    private HashSet<SubgraphId<K>> neighboringSubgraphs;

    private HashMap<I, IVertex<V, E, I, J>> vertices;

    private HashMap<I, IRemoteVertex<V, E, I, J, K>> remoteVertices;

    private GiraphSubgraphConfiguration conf;

    public SubgraphInput(SubgraphId<K> subgraphId, GiraphSubgraphConfiguration conf) {
      this.subgraphId = subgraphId;
      this.conf = conf;
      vertices = new HashMap<>( );
      remoteVertices = new HashMap<>();
      neighboringSubgraphs = new HashSet<>();
    }

    public abstract I decodeId(String s);

    public abstract K decodeSId(String s);

    public void addEntry(String[] values) {
      DefaultSubgraphVertex<V, E, I, J> subgraphVertex = new DefaultSubgraphVertex();
      subgraphVertex.initialize(decodeId(values[1]), getSubgraphVertexValue(), getVertexEdges(values));
      vertices.put(subgraphVertex.getId(), subgraphVertex);
    }

    public abstract V getSubgraphVertexValue();

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

    protected LinkedList<IEdge<E, I, J>> getVertexEdges(String[] values) {
      int i = 3;
      LinkedList<IEdge<E, I, J>> edges = Lists.newLinkedList();
      while (i < values.length) {
        I vertexId = decodeId(values[i]);
        IEdge<E, I, J> e = createVertexEdge(vertexId);
        edges.add(e);
        K remoteSId = decodeSId(values[i + 1]);
        int remotePId = Integer.parseInt(values[i + 2]);
        if (!subgraphId.getSubgraphId().equals(remoteSId)) {
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

    protected Vertex populateSubgraph(Vertex subgraph) {
      SubgraphVertices<S, V, E, I, J, K> subgraphVertices = new SubgraphVertices<>();
      subgraphVertices.setRemoteVertices(remoteVertices);
      subgraphVertices.initialize(vertices);
      subgraphVertices.setSubgraphValue((S) conf.createSubgraphValue());
      subgraphVertices.setSubgraphPartitionMapping(getSubgraphPartitionMapping());
      subgraph.initialize(subgraphId, subgraphVertices, getSubgraphNeighbors());
      return subgraph;
    }

    protected MapWritable getSubgraphPartitionMapping() {
      MapWritable subgraphPartitionMapping = new MapWritable();
      for (SubgraphId<K> subgraphId : neighboringSubgraphs) {
        subgraphPartitionMapping.put(subgraphId.getSubgraphId(), new IntWritable(subgraphId.getPartitionId()));
      }
      return subgraphPartitionMapping;
    }

    protected Iterable<Edge<SubgraphId<K>, NullWritable>> getSubgraphNeighbors() {
      List<Edge<SubgraphId<K>, NullWritable>> edges = Lists.newLinkedList();
      for (SubgraphId<K> sId: neighboringSubgraphs){
        edges.add(EdgeFactory.create(sId, NullWritable.get()));
      }
      return edges;
    }

  }

}

