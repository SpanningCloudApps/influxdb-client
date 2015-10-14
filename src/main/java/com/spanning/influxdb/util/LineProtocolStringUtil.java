package com.spanning.influxdb.util;

import org.apache.commons.lang3.StringUtils;

import java.util.stream.Stream;

/**
 * Utility methods related to InfluxDB line protocol strings.
 */
public class LineProtocolStringUtil {

    /**
     * Escape all spaces and commas in a string.
     *
     * @param rawString The string.
     * @return {@code rawString} with all spaces and commas escaped with a backslash.
     */
    public static String escapeSpacesAndCommas(String rawString) {
        return escapeWithBackslashes(rawString, " ", ",");
    }

    /**
     * Escape all double quotes in a string.
     *
     * @param rawString The string.
     * @return {@code rawString} with all double quotes escaped.
     */
    public static String escapeQuotes(String rawString) {
        return escapeWithBackslashes(rawString, "\"");
    }

    // Escape substrings in a string with backslashes.
    private static String escapeWithBackslashes(String rawString, String... toEscapeStrings) {
        // Escape each of the toEscapeStrings found in rawString.
        return Stream.of(toEscapeStrings).reduce(rawString,
                (escaped, toEscapeString) -> StringUtils.replace(escaped, toEscapeString, "\\" + toEscapeString));
    }

}
