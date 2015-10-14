package com.spanning.influxdb.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.mashape.unirest.request.HttpRequestWithBody;
import com.mashape.unirest.request.body.RequestBodyEntity;
import com.spanning.influxdb.client.InfluxDbClient;
import com.spanning.influxdb.client.exception.InfluxDbHttpQueryException;
import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.client.exception.InfluxDbQueryException;
import com.spanning.influxdb.client.exception.InfluxDbWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.spanning.influxdb.model.QueryResponse;
import com.spanning.influxdb.model.QueryResult;
import org.apache.http.HttpStatus;
import org.jooq.lambda.Seq;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.spanning.test.util.MockitoUtil.mockWithGeneric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InfluxDbHttpClientTest {

    private static final String BASE_URL = "baseUrl";
    private static final String DATABASE = "database";
    private static final String RETENTION_POLICY = "retentionPolicy";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Mock
    private Function<String, HttpRequestWithBody> getRequestFn;

    @Mock
    private ObjectMapper objectMapper;

    private InfluxDbHttpClient influxDbHttpClient;

    @Before
    public void setupClient() {
        // Spy on the InfluxDB client so that we can mock its requestWithAuth method.
        influxDbHttpClient = spy(new InfluxDbHttpClient(
                BASE_URL, Optional.of(new InfluxDbClient.Credentials(USERNAME, PASSWORD)), objectMapper));
    }

    // Test InfluxDbHttpClient.writePoints.
    @Test
    public void testWritePoints() throws InfluxDbWriteException, UnirestException {
        // Mock an HttpResponse indicating the write request was executed successfully.
        HttpResponse<String> response = mockWithGeneric(HttpResponse.class);
        when(response.getStatus()).thenReturn(HttpStatus.SC_NO_CONTENT);
        testWritePoints(invocationOnMock -> response);
    }

    // Test InfluxDbHttpClient.writePoints when an attempt to execute a request throws a UnirestException.
    @Test(expected = InfluxDbWriteException.class)
    public void testWritePointsUnirestException() throws InfluxDbWriteException, UnirestException {
        // Mock a request that throws a UnirestException when executed.
        UnirestException expectedException = new UnirestException("something bad");
        try {
            // Since executing the request throws expectedException, this should throw an InfluxDbWriteException with
            // cause of "expectedException".
            testWritePoints(invocationOnMock -> {
                throw expectedException;
            });
        } catch (InfluxDbWriteException e) {
            assertSame(expectedException, e.getCause());
            throw e;
        }
    }

    // Test InfluxDbHttpClient.writePoints when the status code returned isn't "no content".
    @Test(expected = InfluxDbHttpWriteException.class)
    public void testWritePointsInvalidStatusCode() throws InfluxDbWriteException, UnirestException {
        // Mock an HttpResponse indicating that the write request was not executed successfully.
        HttpResponse<String> response = mockWithGeneric(HttpResponse.class);

        // A 200 "Ok" response should be treated as an error, since it's non-204.
        when(response.getStatus()).thenReturn(HttpStatus.SC_OK);
        when(response.getBody()).thenReturn("Some error message");

        // Attempt to write the points. This should throw an InfluxDbHttpWriteException.
        try {
            // Run the test. Since the response has a non-204 status code, an InfluxDbHttpWriteException should be thrown.
            testWritePoints(invocationOnMock -> response);
        } catch (InfluxDbHttpWriteException e) {
            // Assert the status code/message of the exception.
            assertEquals(response.getStatus(), e.getStatusCode());
            assertEquals(response.getBody(), e.getResponseBody());
            throw e;
        }
    }

    // Test InfluxDbHttpClient.executeQuery.
    @Test
    public void testExecuteQuery() throws InfluxDbQueryException, UnirestException, IOException {
        // Mock an HttpResponse that returns an expected query result.
        String queryResponseString = "queryResultBody";
        HttpResponse response = mock(HttpResponse.class);
        when(response.getBody()).thenReturn(queryResponseString);
        when(response.getStatus()).thenReturn(HttpStatus.SC_OK);

        // Mock objectMapper to parse expectedQueryResultBody to an expected result.
        List<QueryResult> expectedResult = Collections.singletonList(mock(QueryResult.class));
        QueryResponse queryResponse = QueryResponse.jsonCreator(expectedResult, null);
        when(objectMapper.readValue(queryResponseString, QueryResponse.class)).thenReturn(queryResponse);

        // Test executing a query and assert the expected result is returned.
        assertEquals(expectedResult, testExecuteQuery(invocationOnMock -> response));
    }

    // Test that InfluxDbHttpClient.executeQuery throws an InfluxDbQueryException when a UnirestException is thrown
    // while executing a query request.
    @Test(expected = InfluxDbQueryException.class)
    public void testExecuteQueryUnirestException() throws InfluxDbQueryException, UnirestException {
        UnirestException expectedException = new UnirestException("something bad");
        try {
            // Since executing the request throws a UnirestException, expect an InfluxDbQueryException to be thrown
            // with cause of "expectedException".
            testExecuteQuery(invocationOnMock -> {
                throw expectedException;
            });
        } catch (InfluxDbQueryException e) {
            assertSame(expectedException, e.getCause());
            throw e;
        }
    }

    // Test that InfluxDbHttpClient.executeQuery throws an InfluxDbHttpQueryException when the response body is empty.
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteQueryEmptyResponseBody() throws InfluxDbQueryException, UnirestException {
        // Mock an HttpResponse with no response body. No need to mock response.getBody since it returns null by default.
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatus()).thenReturn(HttpStatus.SC_OK);
        try {
            // The response body will be empty, so expect an InfluxDbHttpQueryException to be thrown.
            testExecuteQuery(invocationOnMock -> response);
        } catch (InfluxDbHttpQueryException e) {
            // Assert that the exception has the expected status code.
            assertEquals(response.getStatus(), e.getStatusCode());
            throw e;
        }
    }

    // Test that InfluxDbHttpClient.executeQuery throws an InfluxDbQueryException if an IOException is thrown while
    // attempting to parse the response body.
    @Test(expected = InfluxDbQueryException.class)
    public void testExecuteQueryParseIOException() throws InfluxDbQueryException, UnirestException, IOException {
        // Mock an HttpResponse with a known response body.
        String responseBody = "notParsable";
        HttpResponse response = mock(HttpResponse.class);
        when(response.getBody()).thenReturn(responseBody);
        when(response.getStatus()).thenReturn(HttpStatus.SC_OK);

        // Mock objectMapper to throw an IOException when attempting to parse the response body.
        IOException expectedException = new IOException();
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenThrow(expectedException);

        try {
            // Since parsing the response throws an IOException, expect an InfluxDbQueryException to be thrown with
            // cause of "expectedException".
            testExecuteQuery(invocationOnMock -> response);
        } catch (InfluxDbQueryException e) {
            // Assert that the exception has the expected cause.
            assertSame(expectedException, e.getCause());
            throw e;
        }
    }

    // Test that InfluxDbHttpClient.executeQuery throws an InfluxDbHttpQueryException when a non-200 status code is
    // returned.
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteQueryInvalidStatusCode() throws InfluxDbQueryException, UnirestException, IOException {
        // Mock an HttpResponse with a known response body and 404 "not found" status code.
        String responseBody = "responseBody";
        HttpResponse response = mock(HttpResponse.class);
        when(response.getBody()).thenReturn(responseBody);
        when(response.getStatus()).thenReturn(HttpStatus.SC_NOT_FOUND);

        // Mock objectMapper to parse the response.
        QueryResponse queryResponse = QueryResponse.jsonCreator(Collections.emptyList(), null);
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenReturn(queryResponse);

        try {
            // Since the status code isn't 200 "ok", an InfluxDbHttpQueryException should be thrown.
            testExecuteQuery(invocationOnMock -> response);
        } catch (InfluxDbHttpQueryException e) {
            // Assert that the exception has the expected status code.
            assertEquals(response.getStatus(), e.getStatusCode());
            throw e;
        }
    }

    // Test that InfluxDbHttpClient.executeQuery throws an InfluxDbHttpQueryException when an error is present in the
    // response.
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteQueryErrorInResponseBody() throws InfluxDbQueryException, UnirestException, IOException {
        // Mock an HttpResponse with a known response body.
        String responseBody = "responseBody";
        HttpResponse response = mock(HttpResponse.class);
        when(response.getBody()).thenReturn(responseBody);
        when(response.getStatus()).thenReturn(HttpStatus.SC_OK);

        // Mock objectMapper to parse the response into a QueryResponse containing an error.
        String expectedErrorMessage = "errorMessage";
        QueryResponse queryResponse = QueryResponse.jsonCreator(Collections.emptyList(), expectedErrorMessage);
        when(objectMapper.readValue(responseBody, QueryResponse.class)).thenReturn(queryResponse);

        try {
            // Since the response contains an error, an InfluxDbHttpQueryException should be thrown.
            testExecuteQuery(invocationOnMock -> response);
        } catch (InfluxDbHttpQueryException e) {
            // Assert that the exception has the expected status code/error message.
            assertEquals(response.getStatus(), e.getStatusCode());
            assertEquals(expectedErrorMessage, e.getError());
            throw e;
        }
    }

    // Test InfluxDbHttpClient.requestWithAuth.
    @Test
    public void testRequestWithAuth() {
        String endpoint = "endpoint";

        // Mock the getRequestFn to return a mocked HttpRequestWithBody.
        HttpRequestWithBody request = mock(HttpRequestWithBody.class);
        when(getRequestFn.apply(any())).thenReturn(request);

        // Assert that requestWithAuth returns the expected request.
        assertSame(request, influxDbHttpClient.requestWithAuth(getRequestFn, endpoint));

        // Verify that the basic auth credentials were added to the request.
        verify(request, times(1)).basicAuth(USERNAME, PASSWORD);

        // Verify that the expected url was used to create the request.
        verify(getRequestFn, times(1)).apply(BASE_URL + "/" + endpoint);
    }

    private static DataPoint mockDataPoint(String lineProtocolString) {
        DataPoint dataPoint = mock(DataPoint.class);
        when(dataPoint.lineProtocolString()).thenReturn(lineProtocolString);
        return dataPoint;
    }

    // Test InfluxDbHttpClient.executeQuery.
    private List<QueryResult> testExecuteQuery(Answer<HttpResponse<String>> executeRequestAnswer)
            throws InfluxDbQueryException, UnirestException {
        // Mock a request that will be used to verify that the expected request was made to InfluxDB.
        GetRequest request = mock(GetRequest.class);
        when(request.queryString(any(), anyString())).thenReturn(request);

        // Mock the request to answer with "executeRequestAnswer" when it's executed.
        when(request.asString()).then(executeRequestAnswer);

        // Mock InfluxDbHttpClient.requestWithAuth to return the request.
        doReturn(request).when(influxDbHttpClient)
                .requestWithAuth(InfluxDbHttpClient.GET_FN, InfluxDbHttpClient.Endpoint.QUERY);

        // Perform a query and verify that expected request was executed.
        String query = "influxDbQuery";
        try {
            return influxDbHttpClient.executeQuery(DATABASE, query);
        } finally {
            // Verify the query params.
            verify(request, times(1)).queryString(InfluxDbHttpClient.QueryParam.DATABASE, DATABASE);
            verify(request, times(1)).queryString(InfluxDbHttpClient.QueryParam.QUERY, query);
        }
    }

    // Test InfluxDbHttpClient.writePoints.
    private void testWritePoints(Answer<HttpResponse<String>> executeRequestAnswer)
            throws InfluxDbWriteException, UnirestException {
        // Mock a request that will be used to verify that the expected request was made to InfluxDB.
        HttpRequestWithBody request = mock(HttpRequestWithBody.class);
        when(request.queryString(any(), anyString())).thenReturn(request);

        // Mock the request to answer with "executeRequestAnswer" when it's executed.
        RequestBodyEntity requestBodyEntity = mock(RequestBodyEntity.class);
        when(request.body(anyString())).thenReturn(requestBodyEntity);
        when(requestBodyEntity.asString()).then(executeRequestAnswer);

        // Mock influxDbHttpClient.requestWithAuth to return the request.
        doReturn(request).when(influxDbHttpClient)
                .requestWithAuth(InfluxDbHttpClient.POST_FN, InfluxDbHttpClient.Endpoint.WRITE);

        // Create a list of mocked DataPoints to write to InfluxDB.
        String lineProtocolString = "dataPointLPString";
        List<DataPoint> dataPoints = Seq.generate(() -> mockDataPoint(lineProtocolString)).limit(10).toList();

        // Write the points and verify that the expected request was executed.
        try {
            influxDbHttpClient.writePoints(DATABASE, RETENTION_POLICY, dataPoints);
        } finally {
            // Verify the query params.
            verify(request, times(1)).queryString(InfluxDbHttpClient.QueryParam.DATABASE, DATABASE);
            verify(request, times(1)).queryString(InfluxDbHttpClient.QueryParam.RETENTION_POLICY, RETENTION_POLICY);
            verify(request, times(1)).queryString(InfluxDbHttpClient.QueryParam.PRECISION, InfluxDbClient.MS_PRECISION);

            // Verify the body.
            String expectedBody = Seq.seq(dataPoints).map(DataPoint::lineProtocolString).toString("\n");
            verify(request, times(1)).body(expectedBody);
        }
    }

}
