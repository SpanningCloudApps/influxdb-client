/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtils;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Class representing a field of an InfluxDB data point.
 */
public class Field {
    
    private static final String STRING_FIELD_VALUE_FORMAT = "\"%s\"";

    private final String fieldName;
    private final Object fieldValue;

    public Field(String fieldName, Object fieldValue) {
        this.fieldName = requireNonNull(fieldName, "fieldName can't be null");
        this.fieldValue = requireNonNull(fieldValue, "fieldValue can't be null");
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getFieldValue() {
        return fieldValue;
    }

    /**
     * Get the line protocol string representing this field.
     * @return The line protocol string representing this field.
     * @see <a href="https://influxdb.com/docs/v0.9/write_protocols/line.html#fields">
     *     https://influxdb.com/docs/v0.9/write_protocols/line.html#fields</a>
     */
    public String lineProtocolString() {
        String escapedFieldName = LineProtocolStringUtils.escapeSpacesAndCommas(fieldName);
        return String.join("=", escapedFieldName, getFieldValueString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(fieldName, field.fieldName) &&
                Objects.equals(fieldValue, field.fieldValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, fieldValue);
    }

    private String getFieldValueString() {
        if (fieldValue instanceof String) {
            // The line protocol string for a string field value is the string wrapped in double quotes with quotes within
            // the string escaped.
            return String.format(STRING_FIELD_VALUE_FORMAT, LineProtocolStringUtils.escapeQuotes((String) fieldValue));
        } else if (fieldValue instanceof Number || fieldValue instanceof Boolean) {
            // For booleans and numeric values, use the string value.
            return String.valueOf(fieldValue);
        } else {
            throw new IllegalStateException("Invalid fieldValue type: " + fieldValue.getClass());
        }
    }

    /**
     * Get a {@link Field} for a string value.
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, String fieldValue) {
        return new Field(fieldName, fieldValue);
    }

    /**
     * Get a {@link Field} for a numeric value.
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, Number fieldValue) {
        return new Field(fieldName, fieldValue);
    }

    /**
     * Get a {@link Field} for a boolean value.
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, Boolean fieldValue) {
        return new Field(fieldName, fieldValue);
    }
    
}
