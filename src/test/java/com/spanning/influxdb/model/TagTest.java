package com.spanning.influxdb.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TagTest {

    // Test Tag.lineProtocolString.
    @Test
    public void testLineProtocolString() {
        // Use keys and values with spaces and commas (should be escaped in the resulting string).
        assertEquals("key\\ \\,name=\\,key\\,\\ value", new Tag("key ,name", ",key, value").lineProtocolString());
    }

}
