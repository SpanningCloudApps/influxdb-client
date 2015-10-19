/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TagTest {
    
    @Test
    public void testLineProtocolString() {
        // Use keys and values with spaces and commas (should be escaped in the resulting string).
        assertEquals("key\\ \\,name=\\,key\\,\\ value", new Tag("key ,name", ",key, value").lineProtocolString());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullTagName() {
        new Tag(null, "tagValue");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullTagValue() {
        new Tag("tagName", null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyTagName() {
        new Tag("", "tagValue");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyTagValue() {
        new Tag("tagName", "");
    }
    
}
