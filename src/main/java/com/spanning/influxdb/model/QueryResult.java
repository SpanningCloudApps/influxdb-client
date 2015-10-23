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
