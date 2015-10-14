package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtil;

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
     * (see https://influxdb.com/docs/v0.9/write_protocols/line.html#key)
     *
     * @return An InfluxDB line protocol string.
     */
    public String lineProtocolString() {
        String escapedName = LineProtocolStringUtil.escapeSpacesAndCommas(name);
        String escapedValue = LineProtocolStringUtil.escapeSpacesAndCommas(value);
        return escapedName + "=" + escapedValue;
    }

}
