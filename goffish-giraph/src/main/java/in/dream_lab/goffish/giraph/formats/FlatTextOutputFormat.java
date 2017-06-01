package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraph;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by anirudh on 01/06/17.
 */
public class FlatTextOutputFormat extends
    TextVertexOutputFormat<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class FlatTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
        Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex)
        throws IOException, InterruptedException {
      DefaultSubgraph<NullWritable, NullWritable, NullWritable, LongWritable, NullWritable, LongWritable> subgraph = (DefaultSubgraph) vertex;
      Iterable<IVertex<NullWritable, NullWritable, LongWritable, NullWritable>> localVertices = subgraph.getLocalVertices();
      MapWritable subgraphPartitionMapping = subgraph.getValue().getSubgraphPartitionMapping();
      for (IVertex<NullWritable, NullWritable, LongWritable, NullWritable> localVertex : localVertices) {
        StringBuilder record = new StringBuilder(localVertex.getVertexId() + "\t" + subgraph.getSubgraphId());
        for (IEdge<NullWritable, LongWritable, NullWritable> outEdge : localVertex.getOutEdges()) {
          IVertex<NullWritable, NullWritable, LongWritable, NullWritable> neighborVertex = subgraph.getVertexById(outEdge.getSinkVertexId());
          record.append("\t" + neighborVertex.getVertexId());
          if (neighborVertex.isRemote()) {
            IRemoteVertex remoteVertex = (IRemoteVertex) neighborVertex;
            record.append("\t" + remoteVertex.getSubgraphId() + "\t" + subgraphPartitionMapping.get(remoteVertex.getSubgraphId()));
          } else {
            record.append("\t" + subgraph.getSubgraphId() + "\t" + subgraph.getPartitionId());
          }
        }
        getRecordWriter().write(new Text(String.valueOf(subgraph.getPartitionId())),
            new Text(record.toString()));
      }

    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new FlatTextVertexWriter();
  }
}
