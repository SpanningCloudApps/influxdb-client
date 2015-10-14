package com.spanning.influxdb.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldTest {

    // Test Field.lineProtocolString for a string field.
    @Test
    public void testLineProtocolStringStringField() {
        // Spaces/commas should be replaced in the key and the value should have its quotes escaped and be wrapped in
        // quotes.
        assertEquals("field\\,\\ name\\,\\ =\"\\\"quoted value\\\" one more quote\\\"\"",
                new Field("field, name, ", "\"quoted value\" one more quote\"").lineProtocolString());
    }

    // Test Field.lineProtocolString for an integer field.
    @Test
    public void testLineProtocolStringIntegerField() {
        assertEquals("field\\,\\ name\\,\\ =1234", new Field("field, name, ", 1234).lineProtocolString());
    }

    // Test Field.lineProtocolString for a float field.
    @Test
    public void testLineProtocolStringFloatField() {
        assertEquals("field\\,\\ name\\,\\ =1234.4321", new Field("field, name, ", 1234.4321).lineProtocolString());
    }

    // Test Field.lineProtocolString for a boolean field.
    @Test
    public void testLineProtocolStringBooleanField() {
        assertEquals("field\\,\\ name\\,\\ =true", new Field("field, name, ", true).lineProtocolString());
    }

    // Test Field.lineProtocolString for an unsupported type of object.
    @Test(expected = IllegalStateException.class)
    public void testLineProtocolStringUnsupportedObjectType() {
        // Try to get the line protocol string for an unsupported type of field value. This should never happen as long
        // as the Field.create factory methods are used to create fields.
        new Field("badData", new Object()).lineProtocolString();
    }

}
