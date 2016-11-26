package org.apache.giraph.examples;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 02/11/16.
 */
public class SubgraphMasterCompute extends DefaultMasterCompute {
  @Override
  public void compute() {
    long superstep = getSuperstep();
    if (superstep == 0) {
      setComputation(RemoteVerticesFinder.class);
    } else if (superstep == 1) {
      setComputation(RemoteVerticesFinder2.class);
    } else if (superstep == 2) {
      setComputation(RemoteVerticesFinder3.class);
    } else
      setComputation(SubgraphConnectedComponents.class);
  }
}
