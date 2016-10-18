package org.apache.giraph.graph;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 29/09/16.
 */
public class SubgraphId<S extends WritableComparable> implements WritableComparable{
    private int partitionId;
    private S subgraphId;

    public SubgraphId(S subgraphId, int partitionId) {
        this.partitionId = partitionId;
        this.subgraphId = subgraphId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public S getSubgraphId() {
        return subgraphId;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
