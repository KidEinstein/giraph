package in.dream_lab.goffish.giraph.formats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by anirudh on 26/05/17.
 */
public class LongNullNullAdjacencyListMetaGraphInputFormat extends AdjacencyListMetaGraphInputFormat<LongWritable> {
  @Override
  public LongNullNullAdjacencyListMetaGraphReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongNullNullAdjacencyListMetaGraphReader();
  }

  protected class LongNullNullAdjacencyListMetaGraphReader extends AdjacencyListMetaGraphReader {
    @Override
    protected LongWritable decodeSId(String s) {
      return new LongWritable(Long.parseLong(s));
    }
  }

}
