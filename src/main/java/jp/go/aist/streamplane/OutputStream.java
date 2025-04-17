package jp.go.aist.streamplane;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.UUID;

public class OutputStream implements Serializable {

    private String id;
    private Integer parallelism;
    private Partitioner partitioner;

    OutputStream(Integer parallelism, Partitioner partitioner){
        this.id = UUID.randomUUID().toString();
        this.parallelism = parallelism;
        this.partitioner = partitioner;
    }

    public String getId() {
        return id;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public Partitioner<Integer> getPartitioner() {
        return partitioner;
    }

    public int getNextChannelToSendTo(Object key) {
        return partitioner.partition(key, parallelism);
    }
}
