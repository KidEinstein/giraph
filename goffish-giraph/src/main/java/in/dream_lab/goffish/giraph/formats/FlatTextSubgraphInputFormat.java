package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.giraph.edge.Edge;
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
import java.util.List;

/**
 * Created by anirudh on 15/05/17.
 */
public abstract class FlatTextSubgraphInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable> extends VertexInputFormat<I, V, E> {

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
  protected abstract class FlatTextVertexReader extends VertexReader<I, V, E> {
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
   * @param <T> The resulting type of preprocessing.
   */
  protected abstract class FlatTextVertexReaderFromEachLineProcessed<T> extends
      FlatTextVertexReader {

    @Override
    public final boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public final Vertex<I, V, E> getCurrentVertex() throws IOException,
        InterruptedException {
      // TODO: Changed to subgraph
      Vertex<I, V, E> vertex;
      vertex = getConf().createVertex();
      Text line = getRecordReader().getCurrentValue();
      T processed = preprocessLine(line);
      // Get SID + PID
      I sid = getSId(processed);

      // Initializing internals of a subgraph
      V subgraphVertices = getSubgraphVertices();

      // Get subgraph neighbors
      Iterable<Edge<I, E>> subgraphNeighbors = getSubgraphNeighbors(processed);

      // Initializing the subgraph object (vertex in Giraph's case)
      vertex.initialize(sid, subgraphVertices,
          subgraphNeighbors);
      return vertex;
    }

    public abstract I getSId(T line);

    public abstract IVertex readVertex(T line) throws IOException;


    public abstract V getSubgraphVertices() throws IOException, InterruptedException;

    /**
     * ,
     * Preprocess the line so other methods can easily read necessary
     * information for creating vertex.
     *
     * @param line the current line to be read
     * @return the preprocessed object
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract T preprocessLine(Text line) throws IOException;


//    /**
//     * Reads vertex value from the preprocessed line.
//     *
//     * @param line the object obtained by preprocessing the line
//     * @return the vertex value
//     * @throws IOException exception that can be thrown while reading
//     */
//    protected abstract DoubleWritable getValue(T line) throws IOException;

    /**
     * Reads edges from the preprocessed line.
     *
     * @param line the object obtained by preprocessing the line
     * @return the edges
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract Iterable<IEdge> getVertexEdges(T line) throws IOException;

    protected abstract Iterable<Edge<I, E>> getSubgraphNeighbors(T line) throws IOException;


  }


}

