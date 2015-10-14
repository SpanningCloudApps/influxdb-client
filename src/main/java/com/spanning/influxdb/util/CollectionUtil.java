package com.spanning.influxdb.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Class containing utility methods related to java collections.
 */
public class CollectionUtil {

    /**
     * Create an immutable copy of a {@link List}.
     *
     * @param source The source list.
     * @param <E>    The type of elements in the source list.
     * @return An immutable copy of {@code source}, or an empty list if {@code source} is {@code null}.
     */
    public static <E> ImmutableList<E> immutableCopy(List<E> source) {
        // If source is null, use an empty list.
        return Optional.ofNullable(source)
                .map(ImmutableList::copyOf)
                .orElseGet(ImmutableList::of);
    }

    /**
     * Create an immutable copy of a {@link Map}.
     *
     * @param source The source map.
     * @param <K>    The type of the keys in the map.
     * @param <V>    The type of the values in the map.
     * @return An immutable copy of {@code source}, or an empty list if {@code source} is {@code null}.
     */
    public static <K, V> ImmutableMap<K, V> immutableCopy(Map<K, V> source) {
        // If source is null, use an empty map.
        return Optional.ofNullable(source)
                .map(ImmutableMap::copyOf)
                .orElseGet(ImmutableMap::of);
    }

    /**
     * Get a {@link Collector} that collects a {@link java.util.stream.Stream} to an {@link ImmutableList}.
     *
     * @param <E> The type of elements in the list.
     * @return A {@link Collector}.
     */
    public static <E> Collector<E, ?, ImmutableList<E>> immutableListCollector() {
        return Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf);
    }

}
