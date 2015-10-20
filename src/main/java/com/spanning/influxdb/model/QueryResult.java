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
 * Class representing the result of executing an InfluxDB query.
 */
public class QueryResult {
    
    private final List<Series> series = new ArrayList<>();
    private final String error;
    
    @JsonCreator
    public QueryResult(@JsonProperty("series") List<Series> series, @JsonProperty("error") String error) {
        Optional.ofNullable(series).ifPresent(this.series::addAll);
        this.error = error;
    }

    public List<Series> getSeries() {
        return Collections.unmodifiableList(series);
    }

    public String getError() {
        return error;
    }
    
}
