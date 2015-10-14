package com.spanning.influxdb.model;

import com.google.common.collect.ImmutableList;
import com.spanning.influxdb.util.LineProtocolStringUtil;
import org.jooq.lambda.Seq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Class representing an InfluxDB data point.
 */
public class DataPoint {

    private final String measurementName;
    private final ImmutableList<Tag> tags;
    private final ImmutableList<Field> fields;
    private final Instant instant;

    DataPoint(String measurementName, ImmutableList<Tag> tags, ImmutableList<Field> fields, Instant instant) {
        this.measurementName = measurementName;
        this.tags = tags;
        this.fields = fields;
        this.instant = instant;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public ImmutableList<Tag> getTags() {
        return tags;
    }

    public ImmutableList<Field> getFields() {
        return fields;
    }

    public Instant getInstant() {
        return instant;
    }

    /**
     * Get the InfluxDB line protocol string representing this data point.
     * (see https://influxdb.com/docs/v0.9/write_protocols/line.html)
     *
     * @return The InfluxDB line protocol string representing this data point.
     */
    public String lineProtocolString() {
        // The line protocol string is made up of 3 "sections":
        // 1) The "key" section, a comma-separated list of measurement name and tag strings.
        Seq<String> tagStringsSeq = Seq.seq(tags).map(Tag::lineProtocolString);
        // Escape spaces/commas in the measurement name.
        String escapedMeasurementName = LineProtocolStringUtil.escapeSpacesAndCommas(measurementName);
        String keyString = Seq.of(escapedMeasurementName).concat(tagStringsSeq).join(",");

        // 2) The "fields" section, a comma-separated list of field strings.
        String fieldsString = Seq.seq(fields).map(Field::lineProtocolString).join(",");

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
         *
         * @return A {@link DataPoint}.
         */
        public DataPoint build() {
            return new DataPoint(measurementName, ImmutableList.copyOf(tags), ImmutableList.copyOf(fields), instant);
        }

    }

}
