/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        assertEquals("field\\,\\ name\\,\\ =1234i", new Field("field, name, ", 1234).lineProtocolString());
    }
    
    @Test
    public void testLineProtocolStringLongField() {
        assertEquals("field\\,\\ name\\,\\ =1234i", new Field("field, name, ", 1234L).lineProtocolString());
    }

    @Test
    public void testLineProtocolStringFloatField() {
        assertEquals("field\\,\\ name\\,\\ =1234.4321", new Field("field, name, ", 1234.4321F).lineProtocolString());
    }
    
    @Test
    public void testLineProtocolStringDoubleField() {
        assertEquals("field\\,\\ name\\,\\ =1234.4321", new Field("field, name, ", 1234.4321D).lineProtocolString());
    }

    @Test
    public void testLineProtocolStringBooleanField() {
        assertEquals("field\\,\\ name\\,\\ =true", new Field("field, name, ", true).lineProtocolString());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullFieldName() {
        new Field(null, new Object());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyFieldName() {
        new Field("", new Object());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNullFieldValue() {
        new Field("fieldValue", (Object) null);
    }
    
}
