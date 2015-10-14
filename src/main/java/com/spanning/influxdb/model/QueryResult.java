package com.spanning.influxdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spanning.influxdb.util.CollectionUtil;

import java.util.List;

/**
 * Class representing the result of an InfluxDB query.
 */
public class QueryResult {

    private final ImmutableList<Series> series;
    private final String error;

    QueryResult(ImmutableList<Series> series, String error) {
        this.series = series;
        this.error = error;
    }

    public ImmutableList<Series> getSeries() {
        return series;
    }

    public String getError() {
        return error;
    }

    @JsonCreator
    public static QueryResult jsonCreator(@JsonProperty("series") List<Series> series,
                                          @JsonProperty("error") String error) {
        return new QueryResult(CollectionUtil.immutableCopy(series), error);
    }

}
