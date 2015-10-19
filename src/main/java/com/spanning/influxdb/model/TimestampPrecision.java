/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

/**
 * Enum representing the precision of an InfluxDB data point's timestamp.
 */
public enum TimestampPrecision {
    
    NANOSECONDS("n"),
    MICROSECONDS("u"),
    MILLISECONDS("ms"),
    SECONDS("s"),
    MINUTES("m"),
    HOURS("h");
    
    private final String stringValue;
    
    TimestampPrecision(String stringValue) {
        this.stringValue = stringValue;
    }

    public String getStringValue() {
        return stringValue;
    }
    
}
