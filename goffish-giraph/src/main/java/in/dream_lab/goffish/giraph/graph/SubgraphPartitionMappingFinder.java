package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.giraph.master.SubgraphMasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by anirudh on 18/05/17.
 */
public class SubgraphPartitionMappingFinder extends GiraphSubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  @Override
  public void compute(Vertex<SubgraphId<LongWritable>, SubgraphVertices<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable>, DoubleWritable> vertex, Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
    DefaultSubgraph<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraph = (DefaultSubgraph) vertex;
    MapWritable subgraphPartitionMap = new MapWritable();
    subgraphPartitionMap.put(subgraph.getSubgraphId(), new IntWritable(subgraph.getPartitionId()));
    aggregate(SubgraphMasterCompute.ID, subgraphPartitionMap);
  }
}
