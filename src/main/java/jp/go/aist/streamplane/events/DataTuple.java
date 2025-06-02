package jp.go.aist.streamplane.events;

import jp.go.aist.streamplane.operators.OperatorInstanceInfo;
import org.apache.flink.api.java.tuple.Tuple;

public class DataTuple<T extends Tuple> extends StreamEvent {

    private T data;

    public DataTuple(OperatorInstanceInfo sourceInstanceInfo, Integer destinationInstanceIndex, Integer channelType, T data) {
        super(sourceInstanceInfo, destinationInstanceIndex, channelType);
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
