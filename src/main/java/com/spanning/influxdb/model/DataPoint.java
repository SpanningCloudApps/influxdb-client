/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Class representing an InfluxDB data point.
 */
public class DataPoint {
    
    private final String measurementName;
    private final List<Tag> tags;
    private final List<Field> fields;
    private final Instant instant;

    DataPoint(String measurementName, List<Tag> tags, List<Field> fields, Instant instant) {
        this.measurementName = measurementName;
        this.tags = tags;
        this.fields = fields;
        this.instant = instant;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public List<Tag> getTags() {
        return Collections.unmodifiableList(tags);
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public Instant getInstant() {
        return instant;
    }

    /**
     * Get the InfluxDB line protocol string representing this data point.
     * @return The InfluxDB line protocol string representing this data point.
     * @see <a href="https://influxdb.com/docs/v0.9/write_protocols/line.html">
     *     https://influxdb.com/docs/v0.9/write_protocols/line.html</a>
     */
    public String lineProtocolString() {
        // The line protocol string is made up of 3 "sections":
        // 1) The "key" section, a comma-separated list of measurement name and tag strings.
        Stream<String> tagStringsSeq = tags.stream().map(Tag::lineProtocolString);
        String escapedMeasurementName = LineProtocolStringUtils.escapeSpacesAndCommas(measurementName);
        String keyString = Stream.concat(Stream.of(escapedMeasurementName), tagStringsSeq)
                .collect(Collectors.joining(","));

        // 2) The "fields" section, a comma-separated list of field strings.
        String fieldsString = fields.stream()
                .map(Field::lineProtocolString)
                .collect(Collectors.joining(","));

        // 3) The timestamp section, which is the timestamp, in ms since epoch.
        // Note: InfluxDB can accept different precisions (default is ns), but it's assumed that the client will always
        // write points with a precision of "ms".
        String timestampString = String.valueOf(instant.toEpochMilli());

        // The three line protocol sections are joined with spaces to create a complete data point.
        return String.join(" ", keyString, fieldsString, timestampString);
    }

    /**
     * Class used to build a {@link DataPoint}.
     */
    public static class Builder {
        
        private final String measurementName;
        private final List<Tag> tags = new ArrayList<>();
        private final List<Field> fields = new ArrayList<>();
        private Instant instant = Instant.now();
        
        public Builder(String measurementName) {
            this.measurementName = requireNonNull(measurementName, "measurementName can't be null");
        }

        public Builder withTag(String tagName, String tagValue) {
            tags.add(new Tag(tagName, tagValue));
            return this;
        }

        public Builder withField(String fieldName, String fieldValue) {
            fields.add(Field.create(fieldName, fieldValue));
            return this;
        }

        public Builder withField(String fieldName, Number fieldValue) {
            fields.add(Field.create(fieldName, fieldValue));
            return this;
        }

        public Builder withField(String fieldName, Boolean fieldValue) {
            fields.add(Field.create(fieldName, fieldValue));
            return this;
        }

        public Builder withInstant(Instant instant) {
            this.instant = instant;
            return this;
        }

        /**
         * Build a {@link DataPoint} from this builder.
         * @return A {@link DataPoint}.
         */
        public DataPoint build() {
            if (fields.isEmpty()) {
                throw new IllegalStateException("At least 1 field must be provided.");
            }
            return new DataPoint(measurementName, new ArrayList<>(tags), new ArrayList<>(fields), instant);
        }
        
    }
    
}
