/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client.exception;

/**
 * Exception thrown when an attempt to execute an InfluxDB query fails.
 */
public class InfluxDbHttpQueryException extends RuntimeException {
    
    public static final String MESSAGE_FORMAT =
            "Invalid response received when attempting to query InfluxDB: statusCode=%d, responseBody=%s";
    private final int statusCode;
    private final String errorMessage;
    
    public InfluxDbHttpQueryException(int statusCode, String errorMessage) {
        super(String.format(MESSAGE_FORMAT, statusCode, errorMessage));
        this.statusCode = statusCode;
        this.errorMessage = errorMessage;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
    
}
