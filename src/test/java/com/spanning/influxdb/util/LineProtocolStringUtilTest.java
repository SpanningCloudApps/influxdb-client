package com.spanning.influxdb.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LineProtocolStringUtilTest {

    // Test LineProtocolStringUtil.escapeSpacesAndCommas.
    @Test
    public void testEscapeSpacesAndCommas() {
        assertEquals("hello\\ world\\,\\ InfluxDB\\ is\\ awesome!",
                LineProtocolStringUtil.escapeSpacesAndCommas("hello world, InfluxDB is awesome!"));
    }

    // Test LineProtocolStringUtil.escapeQuotes.
    @Test
    public void testEscapeQuotes() {
        assertEquals("hello \\\"world\\\"", LineProtocolStringUtil.escapeQuotes("hello \"world\""));
    }

}
