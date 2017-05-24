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
public abstract class AdjacencyListFlatTextSubgraphInputFormat<S extends Writable, V extends Writable,
    E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable>
    extends FlatTextSubgraphInputFormat<S, V, E, I, J, K> {
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
      FlatTextVertexReaderFromEachLineProcessed {

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

    public abstract K decodeSId(String s);

    public abstract int decodePId(String s);

    @Override
    public SubgraphId<K> getSId(String[] line) {
      return new SubgraphId<K>(decodeSId(line[2]), decodePId(line[0]));
    }


  }
}
