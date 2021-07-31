package org.dz.homework;

import java.util.Map;

/**
 * A simple DTO for input streaming.
 */
public class InputMessage {
    private String pk;
    private Map<String, Object> data;
    private String beforeData;
    private Map<String, String>  headers;

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public String getBeforeData() {
        return beforeData;
    }

    public void setBeforeData(String beforeData) {
        this.beforeData = beforeData;
    }
}
