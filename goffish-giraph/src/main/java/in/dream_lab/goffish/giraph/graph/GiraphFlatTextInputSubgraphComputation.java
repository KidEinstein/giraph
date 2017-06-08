package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;
import in.dream_lab.goffish.giraph.master.SubgraphMasterCompute;
import in.dream_lab.goffish.giraph.partitionstore.PartitionStoreReader;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by anirudh on 20/05/17.
 */
public class GiraphFlatTextInputSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends GiraphSubgraphComputation<S, I, V, E, M, SV, EI> {

    public static final Logger LOG = Logger.getLogger(GiraphFlatTextInputSubgraphComputation.class);
  @Override
  public void compute(Vertex vertex, Iterable iterable) throws IOException {

//      if(getSuperstep()==0){
          //lazy loading test
          if(!((DefaultSubgraph)vertex).isInitialized()){

              PartitionStoreReader reader=new PartitionStoreReader();
              try {

                  LOG.debug("Calling ReadSubgraph for sgid,"+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get());
                  reader.readSubgraph( "hdfs://orion-00:9000/user/bduser/serialization_check/"+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get()+".ser",(DefaultSubgraph) vertex);
              } catch (URISyntaxException e) {
                  e.printStackTrace();
              }

          }

//      }


        super.compute(vertex, iterable);
  }

  @Override
  public long getSuperstep() {
    return super.getSuperstep() + 3;
  }
}
