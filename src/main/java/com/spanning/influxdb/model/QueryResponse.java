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
