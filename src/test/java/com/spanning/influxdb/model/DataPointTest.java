/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import com.spanning.influxdb.util.LineProtocolStringUtils;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataPointTest {
    
    @Test
    public void testLineProtocolString() {
        long timestamp = Instant.now().toEpochMilli();
        String tagLineProtocolString = "tagLineProtocolString";
        String fieldLineProtocolString = "fieldLineProtocolString";
        String measurementName = " measurementName,";

        // Mock a Tag to indicate its line protocol string is tagLineProtocolString.
        Tag tag = mock(Tag.class);
        when(tag.lineProtocolString()).thenReturn(tagLineProtocolString);

        // Mock a Field to indicate its line protocol string is fieldLineProtocolString.
        Field field = mock(Field.class);
        when(field.lineProtocolString()).thenReturn(fieldLineProtocolString);

        // Create a DataPoint and assert its line protocol string is expected.
        DataPoint dataPoint = new DataPoint(measurementName, Collections.singletonList(tag),
                Collections.singletonList(field), timestamp, TimestampPrecision.MILLISECONDS);
        
        // The only string that is expected to be escaped by DataPoint.lineProtocolString is measurementName, which
        // is why it's the only string in this test that contains a space/comma (tagLineProtocolString and
        // fieldLineProtocolString are the responses of the mocked tag.lineProtocolString/field.lineProtocolString
        // methods, so it's assumed they're already escaped).
        String expectedLineProtocolString = LineProtocolStringUtils.escapeSpacesAndCommas(measurementName) + "," +
                tagLineProtocolString + " " + fieldLineProtocolString + " " + timestamp;
        assertEquals(expectedLineProtocolString, dataPoint.lineProtocolString());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuilderConstructorNullMeasurementName() {
        new DataPoint.Builder(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuilderConstructorEmptyMeasurementName() {
        new DataPoint.Builder("");
    }
    
    @Test(expected = IllegalStateException.class)
    public void testBuildDataPointWithNoFields() {
        // Attempting to build a DataPoint without specifying at least one field should throw an IllegalStateException.
        new DataPoint.Builder("measurementName").build();
    }
    
}
