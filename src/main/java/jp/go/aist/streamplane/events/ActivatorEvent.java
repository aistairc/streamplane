package jp.go.aist.streamplane.events;

import jp.go.aist.streamplane.operators.OperatorInstanceInfo;

public class ActivatorEvent extends StreamEvent {


    public ActivatorEvent(OperatorInstanceInfo sourceInstanceInfo, Integer destinationInstanceIndex, Integer channelType) {
        super(sourceInstanceInfo, destinationInstanceIndex, channelType);
    }
}
