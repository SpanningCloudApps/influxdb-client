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
package com.spanning.influxdb.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.spanning.influxdb.client.InfluxDbClient;
import com.spanning.influxdb.client.exception.InfluxDbHttpQueryException;
import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.spanning.influxdb.model.QueryResponse;
import com.spanning.influxdb.model.QueryResult;
import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link InfluxDbClient} implementation that uses the http(s) protocol.
 */
public class InfluxDbHttpClient implements InfluxDbClient {
    
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbHttpClient.class);
    static final int NO_CONTENT_STATUS_CODE = 204;
    static final String AUTH_HEADER_NAME = "Authorization";
    static final MediaType TEXT_PLAIN = MediaType.parse("text/plain");

    /**
     * InfluxDB endpoints.
     */
    public interface Endpoint {
        String QUERY = "query";
        String WRITE = "write";
    }

    /**
     * Query parameters used when making InfluxDB requests.
     */
    public interface QueryParam {
        String DATABASE = "db";
        String PRECISION = "precision";
        String QUERY = "q";
        String RETENTION_POLICY = "rp";
    }

    protected final String baseUrl;
    protected final Optional<InfluxDbCredentials> credentials;
    protected final OkHttpClient httpClient;
    protected final ObjectMapper objectMapper;

    /**
     * Create an {@link InfluxDbHttpClient} that makes requests without auth credentials. 
     * @param baseUrl The base URL for the InfluxDB http(s) API (e.g., http://localhost:8086).
     */
    public InfluxDbHttpClient(String baseUrl) {
        this(baseUrl, Optional.empty(), new OkHttpClient(), new ObjectMapper());
    }

    /**
     * Create an {@link InfluxDbHttpClient} that makes requests using basic auth credentials.
     * @param baseUrl The base URL for the InfluxDB http(s) API (e.g., http://localhost:8086).
     * @param username The user name to use when making requests.
     * @param password The password to use when making requests.
     */
    public InfluxDbHttpClient(String baseUrl, String username, String password) {
        this(baseUrl, Optional.of(new InfluxDbCredentials(username, password)), new OkHttpClient(), new ObjectMapper());
    }
    
    protected InfluxDbHttpClient(String baseUrl, Optional<InfluxDbCredentials> credentials, OkHttpClient httpClient,
                                 ObjectMapper objectMapper) {
        checkArgument(baseUrl != null, "baseUrl can't be null");
        this.baseUrl = baseUrl;
        this.credentials = credentials;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public void writePoint(String database, DataPoint point) {
        writePoints(database, Optional.empty(), Collections.singletonList(point));
    }

    @Override
    public void writePoint(String database, String retentionPolicy, DataPoint point) {
        writePoints(database, Optional.ofNullable(retentionPolicy), Collections.singletonList(point));
    }

    @Override
    public void writePoints(String database, List<DataPoint> points) {
        writePoints(database, Optional.empty(), points);
    }

    @Override
    public void writePoints(String database, String retentionPolicy, List<DataPoint> points) {
        writePoints(database, Optional.ofNullable(retentionPolicy), points);
    }

    @Override
    public List<QueryResult> executeQuery(String database, String query) {
        checkArgument(!Strings.isNullOrEmpty(database), "database can't be null or empty");
        checkArgument(!Strings.isNullOrEmpty(query), "query can't be null");
        
        // Build the URL.
        HttpUrl url = urlBuilder(Endpoint.QUERY)
                .addQueryParameter(QueryParam.DATABASE, database)
                .addQueryParameter(QueryParam.QUERY, query)
                .build();
        
        // Build the request.
        Request request = requestBuilder(url)
                .get()
                .build();
        
        logger.debug("InfluxDB query request: {}", request);
        
        // Execute the request.
        Response response;
        try {
            response = httpClient.newCall(request).execute();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
        logger.debug("InfluxDB query response: {}", response);
        
        // Parse the response body into a QueryResponse.
        QueryResponse queryResponse;
        try {
            queryResponse = objectMapper.readValue(getResponseBodyString(response), QueryResponse.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
        // If the status code is not successful or there is an error message in the response, throw an exception.
        if (!response.isSuccessful() || queryResponse.hasError()) {
            throw new InfluxDbHttpQueryException(response.code(), queryResponse.getError());
        }
        
        return queryResponse.getResults();
    }

    /**
     * Write {@link DataPoint DataPoints} to InfluxDB in bulk, optionally with a specific retention policy.
     * Note: All {@link DataPoint#timestamp} values for points in the {@code points} list should have the same
     * precision, because the {@link DataPoint#timestampPrecision} value of the first point in the list will be used
     * for all points written.
     * @param database The database to which the points should be written.
     * @param retentionPolicy An optional retention policy (see
     * <a href="https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy">
     *  https://influxdb.com/docs/v0.9/concepts/glossary.html#retention-policy</a>)
     * @param points A list of {@link DataPoint DataPoints}.
     */
    private void writePoints(String database, Optional<String> retentionPolicy, List<DataPoint> points) {
        checkArgument(!Strings.isNullOrEmpty(database), "database can't be null or empty");
        checkArgument(points != null && !points.isEmpty(), "points must contain at least one DataPoint");
        
        // Use the precision from the first point in points.
        String precisionString = points.get(0).getTimestampPrecision().getStringValue();
        
        // Get the line protocol string for each point and join with newlines.
        String lineProtocolString = points.stream()
                .map(DataPoint::lineProtocolString)
                .collect(Collectors.joining("\n"));

        // Build the URL.
        HttpUrl.Builder urlBuilder = urlBuilder(Endpoint.WRITE)
                .addQueryParameter(QueryParam.DATABASE, database)
                .addQueryParameter(QueryParam.PRECISION, precisionString);
        
        // If a retention policy was specified, add it as a query param.
        retentionPolicy.ifPresent(rp -> urlBuilder.addQueryParameter(QueryParam.RETENTION_POLICY, rp));
        
        // Build the request.
        Request request = requestBuilder(urlBuilder.build())
                .post(RequestBody.create(TEXT_PLAIN, lineProtocolString))
                .build();
        
        logger.debug("InfluxDB write request: {}", request);
        
        // Execute the request.
        Response response;
        try {
            response = httpClient.newCall(request).execute();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
        logger.debug("InfluxDB write response: {}", response);
        
        // The response status code should be 204 (no content). If not, throw an exception.
        if (response.code() != NO_CONTENT_STATUS_CODE) {
            logger.debug("Expected {} status code, but got {} in response to InfluxDB write request.",
                    NO_CONTENT_STATUS_CODE, response.code());
            
            // Get the response body as a string, because it probably contains some info about the error that occurred.
            throw new InfluxDbHttpWriteException(response.code(), getResponseBodyString(response));
        }
    }

    /**
     * Get a builder for an InfluxDB API request with basic auth header added, if applicable.
     * @param url The url.
     * @return A {@link Request.Builder}.
     */
    private Request.Builder requestBuilder(HttpUrl url) {
        Request.Builder builder = new Request.Builder().url(url);
        
        // If credentials provided, add a basic auth header to the request builder.
        credentials.ifPresent(credentials ->
                builder.addHeader(AUTH_HEADER_NAME, Credentials.basic(credentials.username, credentials.password)));

        return builder;
    }

    /**
     * Get a URL builder for an InfluxDB API endpoint.
     * @param endpoint The API endpoint.
     * @return A {@link HttpUrl.Builder}.
     */
    private HttpUrl.Builder urlBuilder(String endpoint) {
        return HttpUrl.parse(baseUrl)
                .newBuilder()
                .addPathSegment(endpoint);
    }

    /**
     * Get the response body of a {@link Response} as a string, rethrowing any {@link IOException} that occurs as an
     * {@link UncheckedIOException}.
     * @param response A {@link Response}.
     * @return A string containing the response body, or an empty optional if there was no body.
     */
    private static String getResponseBodyString(Response response) {
        return Optional.ofNullable(response.body())
                .map(InfluxDbHttpClient::getResponseBodyString)
                .orElse("");
    }

    /**
     * Get the string content of a {@link ResponseBody}, rethrowing any {@link IOException} that occurs as an
     * {@link UncheckedIOException}.
     * @param responseBody A {@link ResponseBody}.
     * @return The string value of the response body.
     */
    private static String getResponseBodyString(ResponseBody responseBody) {
        try {
            return responseBody.string();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Class representing InfluxDB credentials.
     */
    static class InfluxDbCredentials {

        final String username;
        final String password;

        public InfluxDbCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

    }
    
}
