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

import com.google.common.base.Strings;
import com.spanning.influxdb.util.LineProtocolStringUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Class representing a field of an InfluxDB data point.
 */
public class Field {
    
    private static final String STRING_FIELD_VALUE_FORMAT = "\"%s\"";

    private final String fieldName;
    private final Object fieldValue;
    
    public Field(String fieldName, String fieldValue) {
        this(fieldName, (Object) fieldValue);
    }
    
    public Field(String fieldName, Number fieldValue) {
        this(fieldName, (Object) fieldValue);
    }
    
    public Field(String fieldName, Boolean fieldValue) {
        this(fieldName, (Object) fieldValue);
    }

    Field(String fieldName, Object fieldValue) {
        checkArgument(!Strings.isNullOrEmpty(fieldName), "fieldName can't be null or empty");
        checkArgument(fieldValue != null, "fieldValue can't be null");
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
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
    
}
