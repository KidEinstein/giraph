package org.apache.giraph.examples;

/**
 * Created by anirudh on 08/02/17.
 */
public class SubgraphMasterComputeTriangleCount extends SubgraphMasterCompute {
  @Override
  public void subgraphCompute() {
    setComputation(SubgraphTriangleCount.class);
  }
}
