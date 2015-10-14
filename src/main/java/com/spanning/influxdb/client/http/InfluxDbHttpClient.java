package com.spanning.influxdb.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.mashape.unirest.request.HttpRequest;
import com.mashape.unirest.request.HttpRequestWithBody;
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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * {@link InfluxDbClient} implementation that uses the http(s) protocol.
 */
public class InfluxDbHttpClient implements InfluxDbClient {

    // Function used to make a POST request.
    static Function<String, HttpRequestWithBody> POST_FN = Unirest::post;

    // Function used to make a GET request.
    static Function<String, GetRequest> GET_FN = Unirest::get;

    // Endpoints.
    public interface Endpoint {
        String QUERY = "query";
        String WRITE = "write";
    }

    // Query parameter names.
    public interface QueryParam {
        String DATABASE = "db";
        String PRECISION = "precision";
        String QUERY = "q";
        String RETENTION_POLICY = "rp";
    }

    private final String baseUrl;
    private final Optional<Credentials> credentials;
    private final ObjectMapper objectMapper;

    /**
     * Create an {@link InfluxDbHttpClient}.
     *
     * @param baseUrl     The base URL for the InfluxDB http(s) API (e.g., http://localhost:8086).
     * @param credentials Optional {@link Credentials} to use when making API requests.
     */
    public InfluxDbHttpClient(String baseUrl, Optional<Credentials> credentials, ObjectMapper objectMapper) {
        this.baseUrl = requireNonNull(baseUrl, "baseUrl can't be null");
        this.credentials = requireNonNull(credentials, "credentials can't be null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper can't be null");
    }

    @Override
    public void writePoints(String database, String retentionPolicy, List<DataPoint> points) throws InfluxDbWriteException {
        // Get the line protocol string for each point and join with newlines.
        String bodyContent = Seq.seq(points).map(DataPoint::lineProtocolString).join("\n");

        HttpResponse<String> response;
        try {
            response = requestWithAuth(POST_FN, Endpoint.WRITE)
                    .queryString(QueryParam.DATABASE, database)
                    .queryString(QueryParam.RETENTION_POLICY, retentionPolicy)
                    .queryString(QueryParam.PRECISION, MS_PRECISION)
                    .body(bodyContent)
                    .asString();
        } catch (UnirestException e) {
            throw new InfluxDbWriteException("Exception occurred while executing write request.", e);
        }

        // The response status code should be 204 (no content). If not, throw an exception.
        if (response.getStatus() != HttpStatus.SC_NO_CONTENT) {
            throw new InfluxDbHttpWriteException(response.getStatus(), response.getBody());
        }
    }

    @Override
    public List<QueryResult> executeQuery(String database, String query) throws InfluxDbQueryException {
        HttpResponse<String> response;
        try {
            response = requestWithAuth(GET_FN, Endpoint.QUERY)
                    .queryString(QueryParam.DATABASE, database)
                    .queryString(QueryParam.QUERY, query)
                    .asString();
        } catch (UnirestException e) {
            throw new InfluxDbQueryException("Exception occurred while executing query request.", e);
        }

        // If the response body is empty, something isn't right, so throw an exception.
        if (Strings.isNullOrEmpty(response.getBody())) {
            throw new InfluxDbHttpQueryException(response.getStatus(), "empty response body");
        }

        // Parse the response into a QueryResponse.
        QueryResponse queryResponse;
        try {
            queryResponse = objectMapper.readValue(response.getBody(), QueryResponse.class);
        } catch (IOException e) {
            throw new InfluxDbQueryException("Unable to parse query response.", e);
        }

        // If a non-200 status code was returned or there was an error, throw an exception.
        if (response.getStatus() != HttpStatus.SC_OK || queryResponse.getError() != null) {
            throw new InfluxDbHttpQueryException(response.getStatus(), queryResponse.getError());
        }
        
        return queryResponse.getResults();
    }

    /**
     * Get a request with basic auth headers added (if {@link #credentials} is non-null).
     *
     * @param requestFn A function that creates an {@link HttpRequestWithBody} from a URL.
     * @param endpoint  The endpoint.
     * @return A {@link HttpRequestWithBody}.
     */
    <R extends HttpRequest> R requestWithAuth(Function<String, R> requestFn, String endpoint) {
        String endpointUrl = String.join("/", baseUrl, endpoint);
        R request = requestFn.apply(endpointUrl);
        credentials.ifPresent(credentials -> request.basicAuth(credentials.getUsername(), credentials.getPassword()));
        return request;
    }

}
