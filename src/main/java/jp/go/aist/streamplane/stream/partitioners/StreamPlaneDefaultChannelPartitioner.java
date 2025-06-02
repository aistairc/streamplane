package jp.go.aist.streamplane.stream.partitioners;

import org.apache.flink.api.common.functions.Partitioner;

public class StreamPlaneDefaultChannelPartitioner implements Partitioner<Integer> {

    @Override
    public int partition(Integer toInstancelIndex, int numPartitions) {
        if(toInstancelIndex >= 0 && toInstancelIndex < numPartitions) {
            return toInstancelIndex;
        } else {
            throw new IllegalArgumentException("Invalid instance index: " + toInstancelIndex);
        }
    }
}
