package org.apache.giraph.examples;

/**
 * Created by anirudh on 02/11/16.
 */
public class SubgraphMasterComputeSingleSourceShortestPathSir extends SubgraphMasterCompute {
  @Override
  public void subgraphCompute() {
    setComputation(SingleSourceShortestPathOnTemplateNoParent.class);
  }
}
