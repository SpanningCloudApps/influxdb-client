package com.spanning.influxdb.client.exception;

/**
 * Exception thrown when an attempt to write data to InfluxDB fails.
 */
public class InfluxDbWriteException extends Exception {

    public InfluxDbWriteException(String message) {
        super(message);
    }

    public InfluxDbWriteException(String message, Throwable cause) {
        super(message, cause);
    }

}
