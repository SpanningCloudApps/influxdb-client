/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client;

import com.spanning.influxdb.model.DataPoint;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Client used to read/write from InfluxDB.
 */
public interface InfluxDbClient {

    /**
     * Write a single {@link DataPoint} to InfluxDB.
     * @param database The database to which the point should be written.
     * @param point A {@link DataPoint}.
     */
    default void writePoint(String database, DataPoint point) {
        writePoints(database, Optional.empty(), Collections.singletonList(point));
    }

    /**
     * Write a single {@link DataPoint} to InfluxDB with a specific retention policy.
     * @param database The database to which the point should be written.
     * @param retentionPolicy The retention policy (see
     * <a href="https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy">
     *  https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy</a>)
     * @param point A {@link DataPoint}.
     */
    default void writePoint(String database, String retentionPolicy, DataPoint point) {
        writePoints(database, Optional.of(retentionPolicy), Collections.singletonList(point));
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk.
     * Note: All {@link DataPoint#timestamp} values for points in the {@code points} list should have the same
     * precision, because the {@link DataPoint#timestampPrecision} value of the first point in the list will be used
     * for all points written.
     * @param database The database to which the points should be written.
     * @param points A list of {@link DataPoint DataPoints}.
     */
    default void writePoints(String database, List<DataPoint> points) {
        writePoints(database, Optional.empty(), points);
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk with a specific retention policy.
     * Note: All {@link DataPoint#timestamp} values for points in the {@code points} list should have the same
     * precision, because the {@link DataPoint#timestampPrecision} value of the first point in the list will be used
     * for all points written.
     * @param database The database to which the points should be written.
     * @param retentionPolicy The retention policy (see
     * <a href="https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy">
     *  https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy</a>)
     * @param points A list of {@link DataPoint DataPoints}.
     */
    default void writePoints(String database, String retentionPolicy, List<DataPoint> points) {
        writePoints(database, Optional.of(retentionPolicy), points);
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk, optionally with a specific retention policy.
     * Note: All {@link DataPoint#timestamp} values for points in the {@code points} list should have the same
     * precision, because the {@link DataPoint#timestampPrecision} value of the first point in the list will be used
     * for all points written.
     * @param database The database to which the points should be written.
     * @param retentionPolicy An optional retention policy (see
     * <a href="https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy">
     *  https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy</a>)
     * @param points A list of {@link DataPoint DataPoints}.
     */
    void writePoints(String database, Optional<String> retentionPolicy, List<DataPoint> points);
    
}
