package com.messaging.network;

import java.io.Serializable;

public class Response implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean success;
    private final Object  data;
    private final String  errorMessage;

    private Response(boolean success, Object data, String errorMessage) {
        this.success = success;
        this.data = data;
        this.errorMessage = errorMessage;
    }

    public static Response ok(Object data) {
        return new Response(true, data, null);
    }

    public static Response error(String message) {
        return new Response(false, null, message);
    }

    public boolean isSuccess()      { return success; }
    public Object  getData()        { return data; }
    public String  getErrorMessage(){ return errorMessage; }
}
