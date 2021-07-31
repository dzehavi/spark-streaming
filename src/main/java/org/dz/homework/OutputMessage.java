package org.dz.homework;

import java.util.Map;

/**
 * A simple DTO for output streaming
 */
public class OutputMessage {
    private Map<String, Object> data;

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}
