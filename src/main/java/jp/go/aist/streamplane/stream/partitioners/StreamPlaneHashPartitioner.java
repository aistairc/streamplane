package jp.go.aist.streamplane.stream.partitioners;

import org.apache.flink.api.common.functions.Partitioner;

public class StreamPlaneHashPartitioner<KEY> implements Partitioner<KEY> {

    @Override
    public int partition(KEY key, int numPartitions) {
        return Math.abs(key.hashCode() % numPartitions);
    }
}
