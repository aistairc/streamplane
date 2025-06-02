package jp.go.aist.streamplane.stream;

import java.io.Serializable;

public class InputStream implements Serializable {

    private String id;
    private Integer keyFieldIndex;

    public InputStream(String id) {
        this(id, null);
    }

    public InputStream(String id, Integer keyFieldIndex) {
        this.id = id;
        this.keyFieldIndex = keyFieldIndex;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getKeyFieldIndex() {
        return keyFieldIndex;
    }

    public void setKeyFieldIndex(Integer keyFieldIndex) {
        this.keyFieldIndex = keyFieldIndex;
    }
}
