package jp.go.aist.streamplane.events;

import jp.go.aist.streamplane.operators.OperatorInstanceInfo;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;

public abstract class StreamEvent implements Serializable {

    private OperatorInstanceInfo sourceInstanceInfo;
    private Integer destinationInstanceIndex;
    private Integer channelType = 0; // 0: native, 1: IMDG

    protected StreamEvent(OperatorInstanceInfo sourceInstanceInfo, Integer destinationInstanceIndex, Integer channelType) {
        this.sourceInstanceInfo = sourceInstanceInfo;
        this.destinationInstanceIndex = destinationInstanceIndex;
        this.channelType = channelType;
    }

    public void setSourceInstanceInfo(OperatorInstanceInfo sourceInstanceInfo) {
        this.sourceInstanceInfo = sourceInstanceInfo;
    }

    public OperatorInstanceInfo getSourceInstanceInfo() {
        return sourceInstanceInfo;
    }

    public Integer getChannelType() {
        return channelType;
    }

    public void setChannelType(Integer channelType) {
        this.channelType = channelType;
    }

    public Integer getDestinationInstanceIndex() {
        return destinationInstanceIndex;
    }

    public void setDestinationInstanceIndex(Integer destinationInstanceIndex) {
        this.destinationInstanceIndex = destinationInstanceIndex;
    }
}
