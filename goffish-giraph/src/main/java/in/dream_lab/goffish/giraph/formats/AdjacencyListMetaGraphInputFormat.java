package in.dream_lab.goffish.giraph.formats;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.python.antlr.op.Sub;

import java.io.IOException;
import java.util.List;

/**
 * Created by anirudh on 26/05/17.
 */
public abstract class AdjacencyListMetaGraphInputFormat<K extends WritableComparable> extends MetaGraphInputFormat<SubgraphId<K>, SubgraphVertices, NullWritable> {
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
  protected abstract class AdjacencyListMetaGraphReader extends TextVertexReaderFromEachLineProcessed<String[]> {

    /** Cached delimiter used for split */
    private String splitValue = null;
    /** Sanitizer from constructor. */
    private final LineSanitizer sanitizer;

    /**
     * Constructor without line sanitizer.
     */
    public AdjacencyListMetaGraphReader() {
      this(null);
    }

    /**
     * Constructor with line sanitizer.
     *
     * @param sanitizer Sanitizer to be used.
     */
    public AdjacencyListMetaGraphReader(LineSanitizer sanitizer) {
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

    @Override
    public SubgraphId<K> getId(String[] line) throws IOException {
      return new SubgraphId<>(decodeSId(line[0]), decodePId(line[1]));
    }

    protected abstract K decodeSId(String s);

    protected int decodePId(String s) {
      return Integer.parseInt(s);
    }

    @Override
    protected SubgraphVertices getValue(String[] line) throws IOException {
      SubgraphVertices subgraphVertices = getConf().createVertexValue();
      GiraphSubgraphConfiguration giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(getConf());
      subgraphVertices.setSubgraphValue(giraphSubgraphConfiguration.createSubgraphValue());
      return subgraphVertices;
    }

    @Override
    protected Iterable<Edge<SubgraphId<K>, NullWritable>> getEdges(String[] line) throws IOException {
      int i = 2;
      List<Edge<SubgraphId<K>, NullWritable>> edges = Lists.newLinkedList();
      while (i < line.length) {
        // TODO: Add subgraph value in our data input format
        edges.add(generateSubgraphEdge(decodeSId(line[i]), decodePId(line[i + 1])));
        i += 2;
      }
      return edges;
    }

    protected Edge<SubgraphId<K>, NullWritable> generateSubgraphEdge(K sid, int pid) {
      SubgraphId<K> subgraphId = new SubgraphId<>(sid, pid);
      Edge <SubgraphId<K>, NullWritable> edge = EdgeFactory.create(subgraphId, NullWritable.get());
      return edge;
    }
  }
}
