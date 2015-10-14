package com.spanning.influxdb.client.exception;

import java.text.MessageFormat;

/**
 * Exception thrown when an invalid HTTP response or error is returned by InfluxDB when attempting to query.
 */
public class InfluxDbHttpQueryException extends InfluxDbQueryException {

    public static final String MESSAGE_FORMAT =
            "Invalid response received while attempting to query InfluxDB: statusCode={0}, error={1}";
    private final int statusCode;
    private final String error;

    public InfluxDbHttpQueryException(int statusCode, String error) {
        super(MessageFormat.format(MESSAGE_FORMAT, statusCode, error));
        this.statusCode = statusCode;
        this.error = error;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getError() {
        return error;
    }

}
