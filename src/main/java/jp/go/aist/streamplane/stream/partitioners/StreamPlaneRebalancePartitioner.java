package jp.go.aist.streamplane.stream.partitioners;

import org.apache.flink.api.common.functions.Partitioner;

public class StreamPlaneRebalancePartitioner implements Partitioner<Void> {

    private int nextChannelToSendTo = 0;

    @Override
    public int partition(Void unused, int numPartitions) {
        this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % numPartitions;
        return this.nextChannelToSendTo;
    }
}
