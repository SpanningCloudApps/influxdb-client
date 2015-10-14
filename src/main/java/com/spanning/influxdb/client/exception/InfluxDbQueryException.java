package com.spanning.influxdb.client.exception;

/**
 * Exception thrown when executing an InfluxDB query fails.
 */
public class InfluxDbQueryException extends Exception {

    public InfluxDbQueryException(String message) {
        super(message);
    }

    public InfluxDbQueryException(String message, Throwable cause) {
        super(message, cause);
    }

}
