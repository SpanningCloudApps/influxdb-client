/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Class representing the response of executing a query request against an InfluxDB database.
 * @see <a href="https://influxdb.com/docs/v0.9/guides/querying_data.html#querying-data-using-the-http-api">
 *     https://influxdb.com/docs/v0.9/guides/querying_data.html#querying-data-using-the-http-api</a>
 */
public class QueryResponse {
    
    private final List<QueryResult> results = new ArrayList<>();
    private final String error;
    
    @JsonCreator
    public QueryResponse(@JsonProperty("results") List<QueryResult> results, @JsonProperty("error") String error) {
        Optional.ofNullable(results).ifPresent(this.results::addAll);
        this.error = error;
    }

    public List<QueryResult> getResults() {
        return Collections.unmodifiableList(results);
    }

    public String getError() {
        return error;
    }
    
    public boolean hasError() {
        return error != null;
    }
    
}
