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
