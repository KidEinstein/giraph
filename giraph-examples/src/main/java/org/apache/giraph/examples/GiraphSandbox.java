package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 */
public class GiraphSandbox extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable> {
    @Override
    public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable> subgraph, Iterable<NullWritable> messages) throws IOException {

        System.out.print("Hello world from the: " +
                subgraph.getId().getSubgraphId() + " who is following:");
        // iterating over vertex's neighbors
        for (Edge<SubgraphId<LongWritable>, DoubleWritable> e : subgraph.getEdges()) {
            System.out.print(" " + e.getTargetVertexId().getSubgraphId());
        }
        System.out.println("");
        // signaling the end of the current BSP computation for the current vertex
        subgraph.voteToHalt();

    }
}
