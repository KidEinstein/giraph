package org.apache.giraph.examples;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 02/11/16.
 */
public class SubgraphMasterComputeSingleSourceShortestPath extends SubgraphMasterCompute {
  @Override
  public void subgraphCompute() {
    long superstep = getSubgraphSuperstep();
    setComputation(SingleSourceShortestPathOnTemplateNoParent.class);
  }
}
