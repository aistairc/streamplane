package jp.go.aist.streamplane;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;

public class DestinationTask implements Serializable {

    private String taskName;
    private Integer parallelism;
    private Partitioner partitioner;

    DestinationTask(String taskName, Integer parallelism, Partitioner partitioner){
        this.taskName = taskName;
        this.parallelism = parallelism;
        this.partitioner = partitioner;
    }

    public String getTaskName() {
        return taskName;
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
