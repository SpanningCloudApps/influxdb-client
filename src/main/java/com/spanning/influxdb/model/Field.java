package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtil;

import static java.util.Objects.requireNonNull;

/**
 * Class representing a field of an InfluxDB data point.
 */
public class Field {

    private final String fieldName;
    private final Object fieldValue;

    Field(String fieldName, Object fieldValue) {
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
     * (see https://influxdb.com/docs/v0.9/write_protocols/line.html#fields)
     *
     * @return The line protocol string representing this field.
     */
    public String lineProtocolString() {
        String escapedFieldName = LineProtocolStringUtil.escapeSpacesAndCommas(fieldName);
        return escapedFieldName + "=" + getFieldValueString();
    }

    private String getFieldValueString() {
        if (fieldValue instanceof String) {
            // The line protocol string for a string field value is the string wrapped in double quotes with quotes within
            // the string escaped.
            return '"' + LineProtocolStringUtil.escapeQuotes((String) fieldValue) + '"';
        } else if (fieldValue instanceof Number || fieldValue instanceof Boolean) {
            // For booleans and numeric values, use the string value.
            return String.valueOf(fieldValue);
        } else {
            throw new IllegalStateException("Invalid fieldValue type: " + fieldValue.getClass());
        }
    }

    /**
     * Get a {@link Field} for a string value.
     *
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, String fieldValue) {
        return new Field(fieldName, fieldValue);
    }

    /**
     * Get a {@link Field} for a numeric value.
     *
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, Number fieldValue) {
        return new Field(fieldName, fieldValue);
    }

    /**
     * Get a {@link Field} for a boolean value.
     *
     * @param fieldName  The field name.
     * @param fieldValue The field value.
     * @return A {@link Field}.
     */
    public static Field create(String fieldName, Boolean fieldValue) {
        return new Field(fieldName, fieldValue);
    }

}
