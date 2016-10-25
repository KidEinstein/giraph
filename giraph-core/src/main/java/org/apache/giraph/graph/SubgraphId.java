package org.apache.giraph.graph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;

/**
 * Created by anirudh on 29/09/16.
 */
public class SubgraphId<S extends WritableComparable> implements WritableComparable{
    private int partitionId;
    private S subgraphId;
    private Class subgraphIdClass;

    public SubgraphId() {
    }

    public SubgraphId(S subgraphId, int partitionId) {
        subgraphIdClass = subgraphId.getClass();
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
        return subgraphId.compareTo(o);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        subgraphId.write(dataOutput);
        dataOutput.writeInt(partitionId);
    }
/*   EITHER DO THIS OR


import java.lang.reflect.ParameterizedType;

    class Foo {

        public bar() {
            ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
            Class type = (Class) superClass.getActualTypeArguments()[0];
            try {
                T t = type.newInstance();
                //Do whatever with t
            } catch (Exception e) {
                // Oops, no default constructor
                throw new RuntimeException(e);
            }
        }
    }

    OR
    make subgraph class abstract and extend it for custom types

*/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Class<S> subgraphIdClass = (Class<S>) GiraphConstants.SUBGRAPH_ID_CLASS.getDefaultClass();
        subgraphId = ReflectionUtils.newInstance(subgraphIdClass, null);
        subgraphId.readFields(dataInput);
        partitionId = dataInput.readInt();
    }
}
