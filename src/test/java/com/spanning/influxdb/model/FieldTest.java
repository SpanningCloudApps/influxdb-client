/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldTest {

    @Test
    public void testLineProtocolStringStringField() {
        // Spaces/commas should be replaced in the key and the value should have its quotes escaped and be wrapped in
        // quotes.
        assertEquals("field\\,\\ name\\,\\ =\"\\\"quoted value\\\" one more quote\\\"\"",
                new Field("field, name, ", "\"quoted value\" one more quote\"").lineProtocolString());
    }

    @Test
    public void testLineProtocolStringIntegerField() {
        assertEquals("field\\,\\ name\\,\\ =1234", new Field("field, name, ", 1234).lineProtocolString());
    }

    @Test
    public void testLineProtocolStringFloatField() {
        assertEquals("field\\,\\ name\\,\\ =1234.4321", new Field("field, name, ", 1234.4321).lineProtocolString());
    }

    @Test
    public void testLineProtocolStringBooleanField() {
        assertEquals("field\\,\\ name\\,\\ =true", new Field("field, name, ", true).lineProtocolString());
    }

    @Test(expected = IllegalStateException.class)
    public void testLineProtocolStringUnsupportedObjectType() {
        // Try to get the line protocol string for an unsupported type of field value. This should never happen as long
        // as the Field.create factory methods are used to create fields.
        new Field("badData", new Object()).lineProtocolString();
    }
    
}
