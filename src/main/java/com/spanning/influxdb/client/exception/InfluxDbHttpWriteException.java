package com.spanning.influxdb.client.exception;

import java.text.MessageFormat;

/**
 * Exception thrown when an invalid HTTP response is returned by InfluxDB when attempting to write data points.
 */
public class InfluxDbHttpWriteException extends InfluxDbWriteException {

    public static final String MESSAGE_FORMAT =
            "Invalid response received when attempting to write data point to InfluxDB: statusCode={0}, responseBody={1}";
    private final int statusCode;
    private final String responseBody;

    public InfluxDbHttpWriteException(int statusCode, String responseBody) {
        super(MessageFormat.format(MESSAGE_FORMAT, statusCode, responseBody));
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
