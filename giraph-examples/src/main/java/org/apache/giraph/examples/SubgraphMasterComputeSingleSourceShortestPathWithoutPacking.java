package org.apache.giraph.examples;

/**
 * Created by anirudh on 02/11/16.
 */
public class SubgraphMasterComputeSingleSourceShortestPathWithoutPacking extends SubgraphMasterCompute {
  @Override
  public void subgraphCompute() {
    long superstep = getSubgraphSuperstep();
    setComputation(SubgraphSingleSourceShortestPathWithoutPacking.class);
  }
}
