/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client;

import com.spanning.influxdb.model.DataPoint;

import java.util.Collections;
import java.util.List;

/**
 * Client used to read/write from InfluxDB.
 */
public interface InfluxDbClient {

    /**
     * Constant representing milliseconds precision. All timestamps of data points written by this client have a
     * precision of milliseconds.
     */
    String MS_PRECISION = "ms";

    /**
     * Write a single {@link DataPoint} to InfluxDB.
     * @param database The database to which the point should be written.
     * @param retentionPolicy The retention policy.
     * @param point A {@link DataPoint}.
     */
    default void writePoint(String database, String retentionPolicy, DataPoint point) {
        writePoints(database, retentionPolicy, Collections.singletonList(point));
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk.
     * @param database The database to which the points should be written.
     * @param retentionPolicy The retention policy.
     * @param points A list of {@link DataPoint DataPoints}.
     */
    void writePoints(String database, String retentionPolicy, List<DataPoint> points);
    
}
