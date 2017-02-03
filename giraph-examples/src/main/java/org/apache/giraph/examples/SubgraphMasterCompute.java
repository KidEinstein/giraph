package org.apache.giraph.examples;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 01/02/17.
 */
public class SubgraphMasterCompute extends DefaultMasterCompute {
  @Override
  public final void compute() {
    long superstep = getSuperstep();
    if (superstep == 0) {
      setComputation(RemoteVerticesFinder.class);
    } else if (superstep == 1) {
      setComputation(RemoteVerticesFinder2.class);
    } else if (superstep == 2) {
      setComputation(RemoteVerticesFinder3.class);
    } else
      subgraphCompute();
  }

  public long getSubgraphSuperstep() {
    return super.getSuperstep() - 3;
  }

  public void subgraphCompute() {
  }
}
