package org.apache.giraph.examples;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 01/02/17.
 */
public class SubgraphMasterComputeConnectedComponents extends SubgraphMasterCompute {
  @Override
  public void subgraphCompute() {
    long superstep = getSubgraphSuperstep();
    setComputation(SubgraphConnectedComponents.class);
  }
}
