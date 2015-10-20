/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.google.common.base.Strings;
import com.spanning.influxdb.util.LineProtocolStringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Class representing an InfluxDB data point.
 */
public class DataPoint {
    
    private final String measurementName;
    private final List<Tag> tags;
    private final List<Field> fields;
    private final long timestamp;
    private final TimestampPrecision timestampPrecision;

    DataPoint(String measurementName, List<Tag> tags, List<Field> fields, long timestamp,
              TimestampPrecision timestampPrecision) {
        this.measurementName = measurementName;
        this.tags = tags;
        this.fields = fields;
        this.timestamp = timestamp;
        this.timestampPrecision = timestampPrecision;
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

    public long getTimestamp() {
        return timestamp;
    }
    
    public TimestampPrecision getTimestampPrecision() {
        return timestampPrecision;
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

        // 3) The timestamp section, which is the string value of the timestamp, in "duration since epoch", where the
        //    duration's precision is either nanoseconds, microseconds, milliseconds, seconds, minutes, or hours. The
        //    duration's precision is determined by the timestampPrecision member.
        String timestampString = String.valueOf(timestamp);

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
        private long timestamp = Instant.now().toEpochMilli();
        private TimestampPrecision timestampPrecision = TimestampPrecision.MILLISECONDS;
        
        public Builder(String measurementName) {
            checkArgument(!Strings.isNullOrEmpty(measurementName), "measurementName can't be null or empty");
            this.measurementName = measurementName;
        }

        public Builder withTag(String tagName, String tagValue) {
            tags.add(new Tag(tagName, tagValue));
            return this;
        }

        public Builder withField(String fieldName, String fieldValue) {
            fields.add(new Field(fieldName, fieldValue));
            return this;
        }

        public Builder withField(String fieldName, Number fieldValue) {
            fields.add(new Field(fieldName, fieldValue));
            return this;
        }

        public Builder withField(String fieldName, Boolean fieldValue) {
            fields.add(new Field(fieldName, fieldValue));
            return this;
        }

        public Builder withTimestamp(long timestamp, TimestampPrecision timestampPrecision) {
            checkArgument(timestampPrecision != null, "timestampPrecision can't be null");
            this.timestamp = timestamp;
            this.timestampPrecision = timestampPrecision;
            return this;
        }

        /**
         * Build a {@link DataPoint} from this builder.
         * @return A {@link DataPoint}.
         */
        public DataPoint build() {
            checkState(!fields.isEmpty(), "Can't build point without fields");
            return new DataPoint(measurementName, new ArrayList<>(tags), new ArrayList<>(fields), timestamp,
                    timestampPrecision);
        }
        
    }
    
}
