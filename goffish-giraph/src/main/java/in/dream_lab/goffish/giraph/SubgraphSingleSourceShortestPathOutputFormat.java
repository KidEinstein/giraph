package in.dream_lab.goffish.giraph;

import in.dream_lab.goffish.api.Subgraph;
import in.dream_lab.goffish.api.SubgraphVertex;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anirudh on 26/01/17.
 */
public class SubgraphSingleSourceShortestPathOutputFormat extends
    TextVertexOutputFormat<SubgraphId<LongWritable>,SubgraphVertices,NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex) throws IOException, InterruptedException {
      Subgraph<NullWritable, LongWritable, NullWritable, LongWritable, NullWritable, LongWritable> subgraph = (Subgraph) vertex;
      Iterable<SubgraphVertex<LongWritable, NullWritable, LongWritable, NullWritable>> vertices = subgraph.getLocalVertices();
      for (SubgraphVertex<LongWritable, NullWritable, LongWritable, NullWritable> entry : vertices) {
        getRecordWriter().write(
            new Text(String.valueOf(entry.getId().get())),
            new Text(entry.getValue().toString()));
      }
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}
