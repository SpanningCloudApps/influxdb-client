/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods related to InfluxDB line protocol strings.
 */
public class LineProtocolStringUtils {

    private static final String ESCAPE_FORMAT = "\\\\%s";

    /**
     * Escape all spaces and commas in a string.
     * @param rawString The string.
     * @return {@code rawString} with all spaces and commas escaped with a backslash.
     */
    public static String escapeSpacesAndCommas(String rawString) {
        return escapeWithBackslashes(rawString, " ", ",");
    }

    /**
     * Escape all double quotes in a string.
     * @param rawString The string.
     * @return {@code rawString} with all double quotes escaped.
     */
    public static String escapeQuotes(String rawString) {
        return escapeWithBackslashes(rawString, "\"");
    }
    
    /**
     * Escape substrings in a string with backslashes.
     * @param rawString The string in which instances of {@code escapedSubstrings} should be replaced.
     * @param escapedSubstrings The substrings to be replaced.
     * @return The escaped string.
     */
    private static String escapeWithBackslashes(String rawString, String ... escapedSubstrings) {
        String escapedSubstringRegex = Stream.of(escapedSubstrings).collect(Collectors.joining("|", "(", ")"));
        return rawString.replaceAll(escapedSubstringRegex, String.format(ESCAPE_FORMAT, "$1"));
    }
    
}
