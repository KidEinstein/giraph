package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by anirudh on 27/09/16.
 */

// TODO: make a edge class and 
public class GiraphSandbox extends SubgraphComputation<LongWritable, LongWritable, DoubleWritable, DoubleWritable, Text, NullWritable, LongWritable> {
    @Override
    public void compute(Subgraph<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraph, Iterable<Text> messages) throws IOException {
        System.out.print("Hello world from the: " +
                subgraph.getId().getSubgraphId() + " who is following:");
        // iterating over vertex's neighbors
        for (Edge<SubgraphId<LongWritable>, DoubleWritable> e : subgraph.getEdges()) {
            System.out.print(" " + e.getTargetVertexId().getSubgraphId());
        }
        System.out.println("");
        System.out.println("Internal contents");
        SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, NullWritable, LongWritable> subgraphVertices = subgraph.getSubgraphVertices();

        for (SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> vertex : subgraphVertices.getVertices()) {
            System.out.println("Vertex: " + vertex.getId());
        }

        // signaling the end of the current BSP computation for the current vertex
        subgraph.voteToHalt();
    }

}
