package jp.go.aist.streamplane.stream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;
import java.util.UUID;

public class OutputStream implements Serializable {

    private String id;
    private Integer parallelism;
    private Partitioner partitioner;
    private Integer keyFieldIndex;

    public OutputStream(Integer parallelism, Partitioner partitioner) {
        this(null, parallelism, partitioner, null);
    }

    public OutputStream(String customId, Integer parallelism, Partitioner partitioner) {
        this(customId, parallelism, partitioner, null);
    }

    public OutputStream(Integer parallelism, Partitioner partitioner, Integer keyFieldIndex) {
        this(null, parallelism, partitioner, keyFieldIndex);
    }

    public OutputStream(String customId, Integer parallelism, Partitioner partitioner, Integer keyFieldIndex) {
        if (customId == null) {
            this.id = UUID.randomUUID().toString();
        } else {
            this.id = customId;
        }
        this.parallelism = parallelism;
        this.partitioner = partitioner;
        this.keyFieldIndex = keyFieldIndex;
    }

    public String getId() {
        return id;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public int getNextChannelToSendTo(Tuple tuple) {
        if(keyFieldIndex != null) {
            return partitioner.partition(tuple.getField(keyFieldIndex), parallelism);
        } else {
            return getNextChannelToSendTo();
        }
    }

    public int getNextChannelToSendTo() { //non keyed partition
        return partitioner.partition(null, parallelism);
    }

    public int getNextChannelToForwardTo(Integer fromInstanceIndex) { //channel forwarder
        return partitioner.partition(fromInstanceIndex, parallelism);
    }
}
