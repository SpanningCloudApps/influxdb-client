/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LineProtocolStringUtilsTest {
    
    @Test
    public void testEscapeSpacesAndCommas() {
        assertEquals("hello\\ world\\,\\ InfluxDB\\ is\\ awesome!",
                LineProtocolStringUtils.escapeSpacesAndCommas("hello world, InfluxDB is awesome!"));
    }
    
    @Test
    public void testEscapeQuotes() {
        assertEquals("hello \\\"world\\\"", LineProtocolStringUtils.escapeQuotes("hello \"world\""));
    }
    
}
