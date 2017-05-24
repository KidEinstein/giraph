package in.dream_lab.goffish.giraph.aggregators;

import org.apache.giraph.aggregators.Aggregator;

/**
 * Created by ravikant on 20/5/17.
 */
//*Partition should be loaded in the previous superstep of its activation
public class PartitionLoadingAggregator implements Aggregator<MapWritable> {
    MapWritable partitionLoadingMap;

    public PartitionLoadingAggregator() {
        partitionLoadingMap = (MapWritable) createInitialValue();
    }

    @Override
    public void aggregate(MapWritable value) {
        partitionLoadingMap.putAll(value);
    }

    @Override
    public MapWritable createInitialValue() {
        return new MapWritable();
    }

    @Override
    public MapWritable getAggregatedValue() {
        return partitionLoadingMap;
    }

    @Override
    public void setAggregatedValue(MapWritable value) {
        partitionLoadingMap = value;
    }

    @Override
    public void reset() {
        partitionLoadingMap = createInitialValue();
    }
}
