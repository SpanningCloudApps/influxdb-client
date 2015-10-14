/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtils;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Class representing an InfluxDB tag.
 */
public class Tag {
    
    private final String name;
    private final String value;

    public Tag(String name, String value) {
        this.name = requireNonNull(name, "name can't be null");
        this.value = requireNonNull(value, "value can't be null");
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    /**
     * Get this tag's representation as an InfluxDB line protocol string (e.g., "tagName=tagValue").
     * @return An InfluxDB line protocol string.
     * @see <a href="https://influxdb.com/docs/v0.9/write_protocols/line.html#key">
     *     https://influxdb.com/docs/v0.9/write_protocols/line.html#key</a>
     */
    public String lineProtocolString() {
        String escapedName = LineProtocolStringUtils.escapeSpacesAndCommas(name);
        String escapedValue = LineProtocolStringUtils.escapeSpacesAndCommas(value);
        return String.join("=", escapedName, escapedValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tag tag = (Tag) o;
        return Objects.equals(name, tag.name) &&
                Objects.equals(value, tag.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
    
}
