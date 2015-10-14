package com.spanning.influxdb.client;

import com.spanning.influxdb.client.exception.InfluxDbQueryException;
import com.spanning.influxdb.client.exception.InfluxDbWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.spanning.influxdb.model.QueryResult;

import java.util.Collections;
import java.util.List;

/**
 * Client used to read/write from InfluxDB.
 */
public interface InfluxDbClient {

    String MS_PRECISION = "ms";

    /**
     * Write a single {@link DataPoint} to InfluxDB.
     *
     * @param database        The database to which the point should be written.
     * @param retentionPolicy The retention policy.
     * @param point           A {@link DataPoint}.
     * @throws InfluxDbWriteException
     */
    default void writePoint(String database, String retentionPolicy, DataPoint point) throws InfluxDbWriteException {
        writePoints(database, retentionPolicy, Collections.singletonList(point));
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk.
     *
     * @param database        The database to which the points should be written.
     * @param retentionPolicy The retention policy.
     * @param points          A list of {@link DataPoint DataPoints}.
     * @throws InfluxDbWriteException
     */
    void writePoints(String database, String retentionPolicy, List<DataPoint> points) throws InfluxDbWriteException;

    /**
     * Execute an InfluxDB query.
     *
     * @param database The database against which the query will be run.
     * @param query    The query string.
     * @return A list of {@link QueryResult}.
     * @throws InfluxDbQueryException
     */
    List<QueryResult> executeQuery(String database, String query) throws InfluxDbQueryException;

    /**
     * Class representing InfluxDB credentials.
     */
    class Credentials {

        private final String username;
        private final String password;

        public Credentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

    }

}
