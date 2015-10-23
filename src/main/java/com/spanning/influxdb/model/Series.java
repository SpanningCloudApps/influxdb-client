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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Class representing a series in an InfluxDB query result.
 */
public class Series {
    
    private final String name;
    private final Map<String, String> tags = new HashMap<>();
    private final List<String> columns = new ArrayList<>();
    private final List<List<Object>> values = new ArrayList<>();
    
    @JsonCreator
    public Series(@JsonProperty("name") String name, @JsonProperty("tags") Map<String, String> tags,
                  @JsonProperty("columns") List<String> columns, @JsonProperty("values") List<List<Object>> values) {
        this.name = name;
        Optional.ofNullable(tags).ifPresent(this.tags::putAll);
        Optional.ofNullable(columns).ifPresent(this.columns::addAll);
        Optional.ofNullable(values).ifPresent(this.values::addAll);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }

    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public List<List<Object>> getValues() {
        return Collections.unmodifiableList(values);
    }
    
}
