/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client.exception;

/**
 * Exception thrown when an invalid HTTP response is returned by InfluxDB when attempting to write data points.
 */
public class InfluxDbHttpWriteException extends RuntimeException {

    public static final String MESSAGE_FORMAT =
            "Invalid response received when attempting to write data point to InfluxDB: statusCode=%d, responseBody=%s";
    private final int statusCode;
    private final String responseBody;

    public InfluxDbHttpWriteException(int statusCode, String responseBody) {
        super(String.format(MESSAGE_FORMAT, statusCode, responseBody));
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getResponseBody() {
        return responseBody;
    }
    
}
