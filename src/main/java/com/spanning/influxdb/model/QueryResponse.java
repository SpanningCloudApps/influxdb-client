package com.spanning.influxdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spanning.influxdb.util.CollectionUtil;

import java.util.List;

/**
 * Class representing the response of executing a query request against an InfluxDB database.
 * (see https://influxdb.com/docs/v0.9/guides/querying_data.html#querying-data-using-the-http-api)
 */
public class QueryResponse {

    private final ImmutableList<QueryResult> results;
    private final String error;

    QueryResponse(ImmutableList<QueryResult> results, String error) {
        this.results = results;
        this.error = error;
    }

    public ImmutableList<QueryResult> getResults() {
        return results;
    }

    public String getError() {
        return error;
    }

    @JsonCreator
    public static QueryResponse jsonCreator(@JsonProperty("results") List<QueryResult> results,
                                            @JsonProperty("error") String error) {
        return new QueryResponse(CollectionUtil.immutableCopy(results), error);
    }

}
