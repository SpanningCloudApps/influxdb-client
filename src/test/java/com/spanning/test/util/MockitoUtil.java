package com.spanning.test.util;

import static org.mockito.Mockito.mock;

public class MockitoUtil {

    /**
     * Create a mock from a raw class and cast to desired generic type.
     *
     * @param rawType A class of type {@link R}.
     * @param <G>     A The desired generic type of the mock (e.g., {@code List<String>}).
     * @param <R>     The raw type to be mocked (e.g., {@code List}).
     * @return A mock of type {@link G}.
     */
    @SuppressWarnings("unchecked")
    public static <G extends R, R> G mockWithGeneric(Class<R> rawType) {
        return (G) mock(rawType);
    }

}
