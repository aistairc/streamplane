package jp.go.aist.streamplane.stream.partitioners;

import org.apache.flink.api.common.functions.Partitioner;

public class StreamPlaneForwardPartitioner implements Partitioner<Integer> {

    @Override
    public int partition(Integer fromInstanceIndex, int numPartitions) {
        if(fromInstanceIndex >= 0) {
            return fromInstanceIndex % numPartitions;
        } else {
            throw new IllegalArgumentException("Invalid instance index: " + fromInstanceIndex);
        }
    }
}
