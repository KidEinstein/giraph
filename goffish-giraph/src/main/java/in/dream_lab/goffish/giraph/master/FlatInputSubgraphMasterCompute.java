package in.dream_lab.goffish.giraph.master;

import in.dream_lab.goffish.giraph.aggregators.SubgraphPartitionMappingAggregator;
import in.dream_lab.goffish.giraph.graph.*;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 17/03/17.
 */
public class FlatInputSubgraphMasterCompute extends DefaultMasterCompute {
  public static final String ID = "SubgraphPartitionMappingAggregator";

  @Override
  public void compute() {
   setComputation(GiraphFlatTextInputSubgraphComputation.class);
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    registerAggregator(ID, SubgraphPartitionMappingAggregator.class);
  }
}
