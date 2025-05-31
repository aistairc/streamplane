package jp.go.aist.streamplane.operators;

import java.io.Serializable;

public class OperatorInstanceInfo implements Serializable {

    private String operatorName;
    private Integer instanceIndex;
    private Integer operatorParallelism;

    public OperatorInstanceInfo(String operatorName, Integer instanceIndex, Integer operatorParallelism) {
        this.setOperatorName(operatorName);
        this.setInstanceIndex(instanceIndex);
        this.setOperatorParallelism(operatorParallelism);
    }


    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public Integer getInstanceIndex() {
        return instanceIndex;
    }

    public void setInstanceIndex(Integer instanceIndex) {
        this.instanceIndex = instanceIndex;
    }

    public Integer getOperatorParallelism() {
        return operatorParallelism;
    }

    public void setOperatorParallelism(Integer operatorParallelism) {
        this.operatorParallelism = operatorParallelism;
    }

    public boolean equals(OperatorInstanceInfo other) {
        return this.operatorName.equals(other.getOperatorName()) && this.instanceIndex.equals(other.getInstanceIndex()) && this.operatorParallelism.equals(other.getOperatorParallelism());
    }
}
