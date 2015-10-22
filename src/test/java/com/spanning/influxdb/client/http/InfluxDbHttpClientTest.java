/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spanning.influxdb.client.exception.InfluxDbHttpQueryException;
import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.spanning.influxdb.model.QueryResponse;
import com.spanning.influxdb.model.QueryResult;
import com.spanning.influxdb.model.TimestampPrecision;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    
    @Mock
    private ObjectMapper objectMapper;
    
    private InfluxDbHttpClient influxDbHttpClient;
    
    @Before
    public void setupClient() {
        Optional<InfluxDbHttpClient.InfluxDbCredentials> credentials =
                Optional.of(new InfluxDbHttpClient.InfluxDbCredentials(USERNAME, PASSWORD));
        influxDbHttpClient = new InfluxDbHttpClient(BASE_URL, credentials, httpClient, objectMapper);
    }
    
    @Test
    public void testWritePoint() throws IOException {
        // When a request is executed using httpClient, answer with a response indicating the request was executed
        // successfully.
        Call call = mockHttpClientResponse(responseAnswer(InfluxDbHttpClient.NO_CONTENT_STATUS_CODE, null));
        
        // Write a data point using the InfluxDB client and verify that the expected request was executed.
        DataPoint point = mockDataPoint("lineProtocolString");
        influxDbHttpClient.writePoint(DATABASE, point);
        
        // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
        // should have been "call", so also verify that it was executed.
        verify(httpClient, times(1)).newCall(writePointRequest(point));
        verify(call, times(1)).execute();
    }
    
    @Test
    public void testWritePointWithRP() throws IOException {
        // When a request is executed using httpClient, answer with a response indicating the request was executed
        // successfully.
        Call call = mockHttpClientResponse(responseAnswer(InfluxDbHttpClient.NO_CONTENT_STATUS_CODE, null));

        // Write a data point using the InfluxDB client and verify that the expected request was executed.
        DataPoint point = mockDataPoint("lineProtocolString");
        influxDbHttpClient.writePoint(DATABASE, RETENTION_POLICY, point);

        // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
        // should have been "call", so also verify that it was executed.
        verify(httpClient, times(1)).newCall(writePointRequest(point, RETENTION_POLICY));
        verify(call, times(1)).execute();
    }
    
    @Test
    public void testWritePoints() throws IOException {
        // When a request is executed using httpClient, answer with a response indicating the request was executed
        // successfully.
        Call call = mockHttpClientResponse(responseAnswer(InfluxDbHttpClient.NO_CONTENT_STATUS_CODE, null));
        
        // Write a list of data points using the InfluxDB client and verify that the expected request was executed.
        List<DataPoint> points = getMockedDataPoints("lineProtocolString");
        influxDbHttpClient.writePoints(DATABASE, points);
        
        // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
        // should have been "call", so also verify that it was executed.
        verify(httpClient, times(1)).newCall(writePointsRequest(points));
        verify(call, times(1)).execute();
    }
    
    @Test
    public void testWritePointsWithRP() throws IOException {
        // When a request is executed using httpClient, answer with a response indicating the request was executed
        // successfully.
        Call call = mockHttpClientResponse(responseAnswer(InfluxDbHttpClient.NO_CONTENT_STATUS_CODE, null));

        // Write a list of data points using the InfluxDB client and verify that the expected request was executed.
        List<DataPoint> points = getMockedDataPoints("lineProtocolString");
        influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, points);

        // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
        // should have been "call", so also verify that it was executed.
        verify(httpClient, times(1)).newCall(writePointsRequest(points, RETENTION_POLICY));
        verify(call, times(1)).execute();
    }
    
    @Test(expected = UncheckedIOException.class)
    public void testWritePointsIOExceptionWhenExecutingRequest() throws IOException {
        // When the write request is executed, throw a known IOException.
        IOException expectedCause = new IOException("something bad happened");
        Call call = mockHttpClientResponse(invocation -> { throw expectedCause; });

        // Write a list of data points using the InfluxDB client.
        List<DataPoint> points = getMockedDataPoints("lineProtocolString");
        try {
            influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, points);
        } catch (UncheckedIOException e) {
            // Assert that the exception has the expected cause.
            assertSame(expectedCause, e.getCause());
            throw e;
        } finally {
            // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
            // should have been "call", so also verify that it was executed.
            verify(httpClient, times(1)).newCall(writePointsRequest(points));
            verify(call, times(1)).execute();
        }
    }
    
    @Test(expected = InfluxDbHttpWriteException.class)
    public void testWritePointsInvalidStatusCode() throws IOException {
        // When a write request is executed, answer with a response with a non-204 status code and error message.
        String responseBody = "Internal server error occurred, oh no!";
        int statusCode = 500;
        Answer<Response> executeWriteRequestAnswer = responseAnswer(
                statusCode, ResponseBody.create(InfluxDbHttpClient.TEXT_PLAIN, responseBody));
        Call call = mockHttpClientResponse(executeWriteRequestAnswer);

        // Write a list of data points using the InfluxDB client.
        List<DataPoint> points = getMockedDataPoints("lineProtocolString");
        try {
            // Since the status isn't "no content", the attempt to write should throw an InfluxDbHttpWriteException.
            influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, points);
        } catch (InfluxDbHttpWriteException e) {
            assertEquals(statusCode, e.getStatusCode());
            assertEquals(responseBody, e.getResponseBody());
            throw e;
        } finally {
            // Verify that a call was retrieved for a write request. The call that was returned by httpClient.newCall
            // should have been "call", so also verify that it was executed.
            verify(httpClient, times(1)).newCall(writePointsRequest(points));
            verify(call, times(1)).execute();
        }
    }

	@Test(expected = IllegalArgumentException.class)
	public void testWritePointsNullDatabase() {
		// Attempt to call writePoint with a null database. This should cause the client to throw an IllegalArgumentException.
		influxDbHttpClient.writePoint(null, mockDataPoint("lineProtocolString"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testWritePointsEmptyDatabase() {
		// Attempt to call writePoint with an empty string as the database argument. This should cause the client to
		// throw an IllegalArgumentException.
		influxDbHttpClient.writePoint("", mockDataPoint("lineProtocolString"));
	}
    
    @Test(expected = IllegalArgumentException.class)
    public void testWritePointsNullPointsList() {
        // Attempt to write a null list of points. This should cause the client to throw an IllegalArgumentException.
        influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testWritePointsEmptyPointsList() {
        // Attempt to write an empty list of points. This should cause the client to throw an IllegalArgumentException.
        influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, Collections.emptyList());
    }
    
    @Test
    public void testExecuteQuery() throws IOException {
        // Mock httpClient to return a successful response with a known response body.
        String responseBody = "responseBody";
        Call call = mockHttpClientResponse(
                responseAnswer(200, ResponseBody.create(InfluxDbHttpClient.TEXT_PLAIN, responseBody)));
        
        // Mock objectMapper to parse responseBody into a known QueryResponse with no error message.
        List<QueryResult> results = Collections.singletonList(mock(QueryResult.class));
        QueryResponse queryResponse = new QueryResponse(results, null);
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenReturn(queryResponse);
        
        // Execute the query using the client and assert the expected results are returned.
        String query = "query";
        assertEquals(results, influxDbHttpClient.executeQuery(DATABASE, query));
        
        // Verify that a call was retrieved for a query request. The call that was returned by httpClient.newCall
        // should have been "call", so also verify that it was executed.
        verify(httpClient, times(1)).newCall(queryRequest(query));
        verify(call, times(1)).execute();
    }
    
    @Test(expected = UncheckedIOException.class)
    public void testExecuteQueryIOExceptionWhenExecutingRequest() throws IOException {
        // When the query request is executed, throw a known IOException.
        IOException expectedCause = new IOException("something bad happened");
        Call call = mockHttpClientResponse(invocation -> { throw expectedCause; });
        
        // Attempt to execute the query.
        String query = "query";
        try {
            influxDbHttpClient.executeQuery(DATABASE, query);
        } catch (UncheckedIOException e) {
            // Assert the exception has the expected cause.
            assertSame(expectedCause, e.getCause());
            throw e;
        } finally {
            // Verify that a call was retrieved for a query request. The call that was returned by httpClient.newCall
            // should have been "call", so also verify that it was executed.
            verify(httpClient, times(1)).newCall(queryRequest(query));
            verify(call, times(1)).execute();
        }
    }
    
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteQueryUnsuccessfulStatusCode() throws IOException {
        // Mock httpClient to return an unsuccessful response with a known status code and response body.
        int statusCode = 400;
        String responseBody = "responseBody";
        Call call = mockHttpClientResponse(
                responseAnswer(statusCode, ResponseBody.create(InfluxDbHttpClient.TEXT_PLAIN, responseBody)));
        
        // Mock objectMapper to parse responseBody into a known QueryResponse containing an error message.
        String errorMessage = "errorMessage";
        QueryResponse queryResponse = new QueryResponse(Collections.emptyList(), errorMessage);
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenReturn(queryResponse);
        
        // Attempt to execute the query.
        String query = "query";
        try {
            influxDbHttpClient.executeQuery(DATABASE, query);
        } catch (InfluxDbHttpQueryException e) {
            // Assert that the exception has the expected status code/error message.
            assertEquals(statusCode, e.getStatusCode());
            assertEquals(errorMessage, e.getErrorMessage());
            throw e;
        } finally {
            // Verify that a call was retrieved for a query request. The call that was returned by httpClient.newCall
            // should have been "call", so also verify that it was executed.
            verify(httpClient, times(1)).newCall(queryRequest(query));
            verify(call, times(1)).execute();
        }
    }
    
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteQueryErrorInResponse() throws IOException {
        // Mock httpClient to return a response with a successful status code and known response body.
        int statusCode = 200;
        String responseBody = "responseBody";
        Call call = mockHttpClientResponse(
                responseAnswer(statusCode, ResponseBody.create(InfluxDbHttpClient.TEXT_PLAIN, responseBody)));
        
        // Mock objectMapper to parse responseBody into a known QueryResponse containing an error message.
        String errorMessage = "errorMessage";
        QueryResponse queryResponse = new QueryResponse(Collections.emptyList(), errorMessage);
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenReturn(queryResponse);
        
        // Attempt to execute the query.
        String query = "query";
        try {
            influxDbHttpClient.executeQuery(DATABASE, query);
        } catch (InfluxDbHttpQueryException e) {
            // Assert that th exception has the expected status code/error message.
            assertEquals(statusCode, e.getStatusCode());
            assertEquals(errorMessage, e.getErrorMessage());
            throw e;
        } finally {
            // Verify that a call was retrieved for a query request. The call that was returned by httpClient.newCall
            // should have been "call", so also verify that it was executed.
            verify(httpClient, times(1)).newCall(queryRequest(query));
            verify(call, times(1)).execute();
        }
    }
	
	@Test(expected = IllegalArgumentException.class)
	public void testExecuteQueryNullDatabase() {
		// Attempt to call executeQuery with a null db argument. The client should throw an IllegalArgumentException.
		influxDbHttpClient.executeQuery(null, "query");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteQueryEmptyDatabase() {
		// Attempt to call executeQuery with an empty db argument. The client should throw an IllegalArgumentException.
		influxDbHttpClient.executeQuery("", "query");
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testExecuteQueryNullQuery() {
		// Attempt to call executeQuery with a null query argument. The client should throw an IllegalArgumentException.
		influxDbHttpClient.executeQuery("db", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteQueryEmptyQuery() {
		// Attempt to call executeQuery with an empty query argument. The client should throw an IllegalArgumentException.
		influxDbHttpClient.executeQuery("db", "");
	}

    /**
     * Mock {@link #httpClient} to respond to all requests with an answer.
     * @param responseAnswer An {@link Answer}.
     * @return A {@link Call}, that when executed, will answer with {@code responseAnswer}. This is also the call that
     * will be returned any time the {@link OkHttpClient#newCall} method is called on {@link #httpClient}.
     */
    private Call mockHttpClientResponse(Answer<Response> responseAnswer) throws IOException {
        // Mock a Call that answers with responseAnswer when executed.
        Call newCall = mock(Call.class);
        when(newCall.execute()).then(responseAnswer);
        
        // Mock httpClient.newCall to return newCall.
        when(httpClient.newCall(any())).thenReturn(newCall);
        return newCall;
    }

    /**
     * Get a list of mocked {@link DataPoint DataPoints} that have a specified line protocol string.
     * @param lineProtocolString The line protocol string.
     * @return A list of {@link DataPoint DataPoints}.
     */
    private static List<DataPoint> getMockedDataPoints(String lineProtocolString) {
        return Stream.generate(() -> mockDataPoint(lineProtocolString))
                .limit(10)
                .collect(Collectors.toList());
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB write request with the given list of data points.
     * @param dataPoints A list of {@link DataPoint}.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private static Request writePointsRequest(List<DataPoint> dataPoints) {
        return argThat(writeRequestMatcher(dataPoints));
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB write request with the given list of data points and
     * retention policy.
     * @param dataPoints A list of {@link DataPoint}.
     * @param retentionPolicy The retention policy.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private static Request writePointsRequest(List<DataPoint> dataPoints, String retentionPolicy) {
        InfluxDbRequestMatcher matcher = writeRequestMatcher(dataPoints)
                .withExpectedQueryParam(InfluxDbHttpClient.QueryParam.RETENTION_POLICY, retentionPolicy);
        return argThat(matcher);
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB write request for a given point.
     * @param dataPoint A {@link DataPoint}.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private static Request writePointRequest(DataPoint dataPoint) {
        return argThat(writeRequestMatcher(Collections.singletonList(dataPoint)));
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB write request for a given point and retention policy.
     * @param dataPoint A {@link DataPoint}.
     * @param retentionPolicy The retention policy.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private static Request writePointRequest(DataPoint dataPoint, String retentionPolicy) {
        InfluxDbRequestMatcher matcher = writeRequestMatcher(Collections.singletonList(dataPoint))
                .withExpectedQueryParam(InfluxDbHttpClient.QueryParam.RETENTION_POLICY, retentionPolicy);
        return argThat(matcher);
    }

    /**
     * Get a {@link Request} argument that matches an InfluxDB query request.
     * @param query The query.
     * @return A {@link Request} to be used as an argument when mocking/verifying a method invocation.
     */
    private static Request queryRequest(String query) {
        InfluxDbRequestMatcher matcher = new InfluxDbRequestMatcher(InfluxDbHttpClient.Endpoint.QUERY, null, "GET")
                .withExpectedQueryParam(InfluxDbHttpClient.QueryParam.QUERY, query);
        return argThat(matcher);
    }

    /**
     * Get a matcher that matches an InfluxDB write request.
     * @param points The points to be written when the request is executed.
     * @return An {@link InfluxDbRequestMatcher}.
     */
    private static InfluxDbRequestMatcher writeRequestMatcher(List<DataPoint> points) {
        String expectedBody = points.stream()
                .map(DataPoint::lineProtocolString)
                .collect(Collectors.joining("\n"));
        return new InfluxDbRequestMatcher(InfluxDbHttpClient.Endpoint.WRITE, expectedBody, "POST")
                .withExpectedQueryParam(InfluxDbHttpClient.QueryParam.PRECISION, TimestampPrecision.MILLISECONDS.getStringValue());
    }

    /**
     * Mock a {@link DataPoint} to return the specified line protocol string.
     * @param lineProtocolString The line protocol string.
     * @return A mocked {@link DataPoint}.
     */
    private static DataPoint mockDataPoint(String lineProtocolString) {
        DataPoint dataPoint = mock(DataPoint.class);
        when(dataPoint.lineProtocolString()).thenReturn(lineProtocolString);
        when(dataPoint.getTimestampPrecision()).thenReturn(TimestampPrecision.MILLISECONDS);
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
     * Class used to match an InfluxDB API request.
     */
    private static class InfluxDbRequestMatcher extends BaseMatcher<Request> {
        
        private Optional<String> failureDescription = Optional.empty();
        private final String endpoint;
        private final String body;
        private final String method;
        private final Map<String, String> expectedQueryParams = new HashMap<String, String>() {{
            put(InfluxDbHttpClient.QueryParam.DATABASE, DATABASE);
        }};

        public InfluxDbRequestMatcher(String endpoint, String body, String method) {
            this.endpoint = endpoint;
            this.body = body;
            this.method = method;
        }

        @Override
        public boolean matches(Object o) {
            failureDescription = Optional.empty();

            if (!(o instanceof Request)) {
                failureDescription = Optional.of("Request object");
                return false;
            }
            
            Request request = (Request) o;
            
            // Check that the request method is expected.
            if (!method.equals(request.method())) {
                failureDescription = Optional.of(String.format("Request with method=%s", method));
                return false;
            }
            
            // If body is expected, check the request body.
            if (body != null) {
                String requestBody;
                try (Buffer bodyBuffer = new Buffer()) {
                    request.body().writeTo(bodyBuffer);
                    requestBody = bodyBuffer.readByteString().utf8();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Check that the request body is expected.
                if (!body.equals(requestBody)) {
                    failureDescription = Optional.of(String.format("Request with body=%s", body));
                    return false;
                }
            }

            // Check that the request URL starts with BASE_URL + "/" + endpoint.
            String expectedStartsWith = String.join("/", BASE_URL, endpoint);
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
            return expectedQueryParams.entrySet().stream()
                    // Map to string that should be contains in the query string.
                    .map(entry -> String.join("=", entry.getKey(), entry.getValue()))
                    .allMatch(expectedContainsString -> checkQueryString(requestUrlQuery, expectedContainsString));
        }

        @Override
        public void describeTo(Description description) {
            failureDescription.ifPresent(description::appendText);
        }
        
        public InfluxDbRequestMatcher withExpectedQueryParam(String name, String value) {
            expectedQueryParams.put(name, value);
            return this;
        }

        private boolean checkQueryString(String queryString, String expectedContainsString) {
            if (!queryString.contains(expectedContainsString)) {
                failureDescription = Optional.of(String.format(
                        "Request with query string containing %s", expectedContainsString));
                return false;
            }
            return true;
        }
        
    }
    
}
