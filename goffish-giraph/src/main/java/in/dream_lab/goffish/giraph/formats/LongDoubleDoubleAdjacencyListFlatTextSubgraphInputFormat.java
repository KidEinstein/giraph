package in.dream_lab.goffish.giraph.formats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.validation.constraints.Null;
import java.io.DataOutput;
import java.io.IOException;

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
    public LongWritable decodeId(String s) {
      return new LongWritable(Long.parseLong(s));
    }

    @Override
    public LongWritable decodeSId(String s) {
      return new LongWritable(Long.parseLong(s));
    }

    @Override
    public int decodePId(String s) {
      return Integer.parseInt(s);
    }

    @Override
    public NullWritable decodeValue(String s) {
      return NullWritable.get();
    }
  }
}

