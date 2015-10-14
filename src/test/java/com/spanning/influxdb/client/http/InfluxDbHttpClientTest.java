/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client.http;

import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.squareup.okhttp.Call;
import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Protocol;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import okio.Buffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InfluxDbHttpClientTest {

    private static final String BASE_URL = "http://localhost:8086";
    private static final String DATABASE = "database";
    private static final String RETENTION_POLICY = "retentionPolicy";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    
    @Mock
    private OkHttpClient httpClient;
    
    private InfluxDbHttpClient influxDbHttpClient;
    
    @Before
    public void setupClient() {
        influxDbHttpClient = new InfluxDbHttpClient(
                BASE_URL, Optional.of(new InfluxDbHttpClient.InfluxDbCredentials(USERNAME, PASSWORD)), httpClient);
    }
    
    @Test
    public void testWritePoints() throws IOException {
        // When a write request is executed, answer with a response indicating the request was executed successfully.
        Answer<Response> executeWriteRequestAnswer = responseAnswer(InfluxDbHttpClient.NO_CONTENT_STATUS_CODE, null);

        // Since the request is executed successfully, no exception should be thrown.
        writePoints(executeWriteRequestAnswer);
    }
    
    @Test(expected = UncheckedIOException.class)
    public void testWritePointsIOExceptionWhenExecutingRequest() throws IOException {
        // When the write request is executed, throw a known IOException.
        IOException expectedCause = new IOException("something bad happened");
        
        try {
            writePoints(invocation -> { throw expectedCause; });
        } catch (UncheckedIOException e) {
            // Assert that the exception has the expected cause.
            assertSame(expectedCause, e.getCause());
            throw e;
        }
    }
    
    @Test(expected = InfluxDbHttpWriteException.class)
    public void testWritePointsInvalidStatusCode() throws IOException {
        // When a write request is executed, answer with a response with a non-204 status code and error message.
        String responseBody = "Internal server error occurred, oh no!";
        int statusCode = 500;
        Answer<Response> executeWriteRequestAnswer = responseAnswer(
                statusCode, ResponseBody.create(InfluxDbHttpClient.TEXT_PLAIN, responseBody));
        
        try {
            // Since the status isn't "no content", the attempt to write should throw an InfluxDbHttpWriteException.
            writePoints(executeWriteRequestAnswer);
        } catch (InfluxDbHttpWriteException e) {
            assertEquals(statusCode, e.getStatusCode());
            assertEquals(responseBody, e.getResponseBody());
            throw e;
        }
    }
    
    /**
     * Attempt to write points to InfluxDB using {@link #influxDbHttpClient}.
     * @param responseAnswer The answer describing how {@link #influxDbHttpClient} should response to a write request.
     */
    private void writePoints(Answer<Response> responseAnswer) throws IOException {
        // Create a list of mocked DataPoints to write to InfluxDB.
        String lineProtocolString = "dataPointLPString";
        List<DataPoint> dataPoints = Stream.generate(() -> mockDataPoint(lineProtocolString))
                .limit(10)
                .collect(Collectors.toList());
        
        // Mock a Call that answers with responseAnswer when executed.
        Call writeCall = mock(Call.class);
        when(writeCall.execute()).then(responseAnswer);
        
        // Mock influxDbHttpClient to return writeCall when a new call is created for the expected write request.
        when(httpClient.newCall(writePointsRequest(dataPoints))).thenReturn(writeCall);
        
        // Write the points.
        influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, dataPoints);
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB write request with the given list of data points.
     * @param dataPoints A list of {@link DataPoint}.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private Request writePointsRequest(List<DataPoint> dataPoints) {
        return argThat(new WriteRequestMatcher(dataPoints));
    }

    /**
     * Mock a {@link DataPoint} to return the specified line protocol string.
     * @param lineProtocolString The line protocol string.
     * @return A mocked {@link DataPoint}.
     */
    private static DataPoint mockDataPoint(String lineProtocolString) {
        DataPoint dataPoint = mock(DataPoint.class);
        when(dataPoint.lineProtocolString()).thenReturn(lineProtocolString);
        return dataPoint;
    }

    /**
     * Get an {@link Answer} used to respond to executing an InfluxDB API request.
     * @param statusCode The response status code.
     * @param body The response body.
     * @return An {@link Answer}.
     */
    private static Answer<Response> responseAnswer(int statusCode, ResponseBody body) {
        // Note: Can't mock the Response class since it's final, so using a junk request member since it's required to
        // build a Response and because the client doesn't care about the response's request member.
        Request request = new Request.Builder()
                .url(BASE_URL)
                .build();
        
        // Answer with a response containing the specified status/body.
        return invocation -> new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(statusCode)
                .body(body)
                .build();
    }

    /**
     * Matcher that matches an InfluxDB write request for specific {@link DataPoint DataPoints}.
     */
    private static class WriteRequestMatcher extends BaseMatcher<Request> {
        
        private static final Map<String, String> EXPECTED_QUERY_PARAMS = new HashMap<String, String>() {{
            put(InfluxDbHttpClient.QueryParam.DATABASE, DATABASE);
            put(InfluxDbHttpClient.QueryParam.RETENTION_POLICY, RETENTION_POLICY);
            put(InfluxDbHttpClient.QueryParam.PRECISION, InfluxDbHttpClient.MS_PRECISION);
        }};
        
        private final List<DataPoint> dataPoints;
        private Optional<String> failureDescription = Optional.empty();

        public WriteRequestMatcher(List<DataPoint> dataPoints) {
            this.dataPoints = dataPoints;
        }

        @Override
        public boolean matches(Object o) {
            failureDescription = Optional.empty();
            
            if (!(o instanceof Request)) {
                failureDescription = Optional.of("Request object");
                return false;
            }
            
            Request request = (Request) o;
            
            // Build the expected request body.
            String expectedBody = dataPoints.stream()
                    .map(DataPoint::lineProtocolString)
                    .collect(Collectors.joining("\n"));

            // Get the actual request body.
            String body;
            try (Buffer bodyBuffer = new Buffer()) {
                request.body().writeTo(bodyBuffer);
                body = bodyBuffer.readByteString().utf8();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            // Check that the request body is expected.
            if (!expectedBody.equals(body)) {
                failureDescription = Optional.of(String.format("Request with body=%s", expectedBody));
                return false;
            }
            
            // Check that the request URL starts with BASE_URL + "/" + write endpoint.
            String expectedStartsWith = String.join("/", BASE_URL, InfluxDbHttpClient.Endpoint.WRITE);
            HttpUrl requestUrl = request.httpUrl();
            if (!requestUrl.toString().startsWith(expectedStartsWith)) {
                failureDescription = Optional.of(String.format("Request starting with %s", expectedStartsWith));
                return false;
            }
            
            // Check that the request has the correct credentials.
            String expectedAuthHeaderValue = Credentials.basic(USERNAME, PASSWORD); 
            if (!expectedAuthHeaderValue.equals(request.header(InfluxDbHttpClient.AUTH_HEADER_NAME))) {
                failureDescription = Optional.of(
                        String.format("Expected basic auth header value of %s", expectedAuthHeaderValue));
                return false;
            }
            
            // Check the the request URL contains the expected query parameters.
            String requestUrlQuery = requestUrl.query();
            return EXPECTED_QUERY_PARAMS.entrySet().stream()
                    // Map to string that should be contains in the query string.
                    .map(entry -> String.join("=", entry.getKey(), entry.getValue()))
                    .allMatch(expectedContainsString -> checkQueryString(requestUrlQuery, expectedContainsString));
        }
        
        private boolean checkQueryString(String queryString, String expectedContainsString) {
            if (!queryString.contains(expectedContainsString)) {
                failureDescription = Optional.of(String.format(
                        "Request with query string containing %s", expectedContainsString));
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            failureDescription.ifPresent(description::appendText);
        }
        
    }
    
}
