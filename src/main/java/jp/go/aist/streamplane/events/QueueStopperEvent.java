package jp.go.aist.streamplane.events;

import jp.go.aist.streamplane.operators.OperatorInstanceInfo;

public class QueueStopperEvent extends StreamEvent{

    public QueueStopperEvent(OperatorInstanceInfo sourceInstanceInfo) {
        super(sourceInstanceInfo, sourceInstanceInfo.getInstanceIndex(), 1);
    }

}
