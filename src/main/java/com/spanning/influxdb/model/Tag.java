/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.google.common.base.Strings;
import com.spanning.influxdb.util.LineProtocolStringUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Class representing an InfluxDB tag.
 */
public class Tag {
    
    private final String name;
    private final String value;

    public Tag(String name, String value) {
        checkArgument(!Strings.isNullOrEmpty(name), "name can't be null or empty");
        checkArgument(!Strings.isNullOrEmpty(value), "value can't be null or empty");
        this.name = name;
        this.value = value;
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
    
}
