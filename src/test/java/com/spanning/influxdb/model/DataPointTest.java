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
        Instant instant = Instant.now();
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
                Collections.singletonList(field), instant);
        String expectedLineProtocolString = LineProtocolStringUtils.escapeSpacesAndCommas(measurementName) + "," +
                tagLineProtocolString + " " + fieldLineProtocolString + " " + instant.toEpochMilli();
        assertEquals(expectedLineProtocolString, dataPoint.lineProtocolString());
    }
    
}
