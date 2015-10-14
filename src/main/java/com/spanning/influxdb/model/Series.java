package com.spanning.influxdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spanning.influxdb.util.CollectionUtil;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Class representing a series in an InfluxDB query result.
 */
public class Series {

    private final String name;
    private final ImmutableMap<String, String> tags;
    private final ImmutableList<String> columns;
    private final ImmutableList<ImmutableList<Object>> values;

    Series(String name, ImmutableMap<String, String> tags, ImmutableList<String> columns,
           ImmutableList<ImmutableList<Object>> values) {
        this.name = name;
        this.tags = tags;
        this.columns = columns;
        this.values = values;
    }

    public String getName() {
        return name;
    }

    public ImmutableList<String> getColumns() {
        return columns;
    }

    public ImmutableList<ImmutableList<Object>> getValues() {
        return values;
    }
    
    public ImmutableMap<String, String> getTags() {
        return tags;
    }

    @JsonCreator
    public static Series jsonCreator(@JsonProperty("name") String name,
                                     @JsonProperty("tags") Map<String, String> tags,
                                     @JsonProperty("columns") List<String> columns,
                                     @JsonProperty("values") List<List<Object>> values) {
        requireNonNull(name, "name should never be null");

        // Make an immutable copy of the values.
        ImmutableList<ImmutableList<Object>> immutableValues = values.stream()
                .map(CollectionUtil::immutableCopy)
                .collect(CollectionUtil.immutableListCollector());

        return new Series(name, CollectionUtil.immutableCopy(tags), CollectionUtil.immutableCopy(columns),
                immutableValues);
    }

}
