/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
