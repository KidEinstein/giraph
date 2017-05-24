package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Created by anirudh on 18/05/17.
 */
public class LongDoubleDoubleAdjacencyListFlatTextSubgraphInputFormat extends AdjacencyListFlatTextSubgraphInputFormat<NullWritable, NullWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> {
  @Override
  public AdjacencyListFlatTextSubgraphReader createVertexReader(InputSplit split, TaskAttemptContext context) {
    return new LongDoubleDoubleAdjacencyListFlatSubgraphReader();
  }

  protected class LongDoubleDoubleAdjacencyListFlatSubgraphReader extends AdjacencyListFlatTextSubgraphReader {
    @Override
    public LongWritable decodeSId(String s) {
      return new LongWritable(Long.parseLong(s));
    }

    @Override
    public int decodePId(String s) {
      return Integer.parseInt(s);
    }

    @Override
    public SubgraphInput createSubgraphInput(GiraphSubgraphConfiguration conf, SubgraphId<LongWritable> sid) {
      return new LongDoubleDoubleSubgraphInput(conf, sid);
    }
  }

  protected class LongDoubleDoubleSubgraphInput extends SubgraphInput {

    public LongDoubleDoubleSubgraphInput(GiraphSubgraphConfiguration conf, SubgraphId<LongWritable> subgraphId) {
      super(subgraphId, conf);
    }

    @Override
    public NullWritable getSubgraphVertexValue() {
      return NullWritable.get();
    }

    @Override
    public LongWritable decodeId(String s) {
      return new LongWritable(Long.parseLong(s));
    }

    @Override
    public LongWritable decodeSId(String s) {
      return new LongWritable(Long.parseLong(s));
    }
  }
}

