package com.spanning.influxdb.util;

import org.jooq.lambda.Seq;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class CollectionUtilTest {

    // Test CollectionUtil.immutableCopy(List) for a non-null list.
    @Test
    public void testImmutableCopyNonNullList() {
        List<Object> expectedList = Seq.generate(Object::new).limit(10).toList();
        assertEquals(expectedList, CollectionUtil.immutableCopy(expectedList));
    }

    // Test CollectionUtil.immutableCopy(List) for a null list.
    @Test
    public void testImmutableCopyNullList() {
        assertEquals(Collections.emptyList(), CollectionUtil.immutableCopy((List) null));
    }

    // Test CollectionUtil.immutableCopy(Map) for a non-null map.
    @Test
    public void testImmutableCopyNonNullMap() {
        Map<Object, Object> expectedMap =
                Seq.generate(Object::new).limit(10).toMap(Function.identity(), Function.identity());
        assertEquals(expectedMap, CollectionUtil.immutableCopy(expectedMap));
    }

    // Test CollectionUtil.immutableCopy(Map) for a null map.
    @Test
    public void testImmutableCopyNullMap() {
        assertEquals(Collections.emptyMap(), CollectionUtil.immutableCopy((Map) null));
    }

    // Test CollectionUtil.immutableListCollector.
    @Test
    public void testImmutableListCollector() {
        List<Object> expectedList = Seq.generate(Object::new).limit(10).toList();
        assertEquals(expectedList, expectedList.stream().collect(CollectionUtil.immutableListCollector()));
    }

}
