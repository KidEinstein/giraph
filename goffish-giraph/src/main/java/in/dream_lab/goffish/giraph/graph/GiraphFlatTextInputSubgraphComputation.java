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

    static{

    }

  @Override
  public void compute(Vertex vertex, Iterable iterable) throws IOException {

//      final String SERIALIZED_INPUT_PATH = "giraph.serialized.input.path";
////      if(getSuperstep()==0){
//          //lazy loading test
//          if(!((DefaultSubgraph)vertex).isInitialized()){
//
//              PartitionStoreReader reader=new PartitionStoreReader();
//              try {
//                  //TODO:get the path from conf
//                   String hdfspath=getConf(SERIALIZED_INPUT_PATH);
////                  LOG.debug("Calling ReadSubgraph,superstep" +getSuperstep() +",sgid,"+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get()+" in dir "+hdfspath);
//                  long startTime=System.currentTimeMillis();
////                  reader.readSubgraph( "hdfs://orion-00:9000/user/bduser/lazy_loading/ORKT-40/"+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get()+".ser",(DefaultSubgraph) vertex);
//                  reader.readSubgraph( hdfspath+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get()+".ser",(DefaultSubgraph) vertex);
//                  LOG.debug("Calling ReadSubgraph,superstep," +getSuperstep() +",sgid,"+((LongWritable)((DefaultSubgraph) vertex).getSubgraphId()).get()+",pid,"+((DefaultSubgraph) vertex).getPartitionId()+",in_dir,"+hdfspath+",took,"+(System.currentTimeMillis()-startTime));
//              } catch (URISyntaxException e) {
//                  e.printStackTrace();
//              }
//
//          }

//      }


        super.compute(vertex, iterable);
  }

  @Override
  public long getSuperstep() {
    return super.getSuperstep() + 3;
  }
}
