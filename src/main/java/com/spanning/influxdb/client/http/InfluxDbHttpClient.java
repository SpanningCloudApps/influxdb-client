/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client.http;

import com.spanning.influxdb.client.InfluxDbClient;
import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.model.DataPoint;
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * {@link InfluxDbClient} implementation that uses the http(s) protocol.
 */
public class InfluxDbHttpClient implements InfluxDbClient {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDbHttpClient.class);
    static final int NO_CONTENT_STATUS_CODE = 204;
    static final String AUTH_HEADER_NAME = "Authorization";
    static final MediaType TEXT_PLAIN = MediaType.parse("text/plain");

    /**
     * InfluxDB endpoints.
     */
    public interface Endpoint {
        String WRITE = "write";
    }

    /**
     * Query parameters used when making InfluxDB requests.
     */
    public interface QueryParam {
        String DATABASE = "db";
        String PRECISION = "precision";
        String RETENTION_POLICY = "rp";
    }

    private final String baseUrl;
    private final Optional<InfluxDbCredentials> credentials;
    private final OkHttpClient httpClient;

    /**
     * Create an {@link InfluxDbHttpClient} that makes requests without auth credentials. 
     * @param baseUrl The base URL for the InfluxDB http(s) API (e.g., http://localhost:8086).
     */
    public InfluxDbHttpClient(String baseUrl) {
        this(baseUrl, Optional.empty(), new OkHttpClient());
    }

    /**
     * Create an {@link InfluxDbHttpClient} that makes requests using basic auth credentials.
     * @param baseUrl The base URL for the InfluxDB http(s) API (e.g., http://localhost:8086).
     * @param username The user name to use when making requests.
     * @param password The password to use when making requests.
     */
    public InfluxDbHttpClient(String baseUrl, String username, String password) {
        this(baseUrl, Optional.of(new InfluxDbCredentials(username, password)), new OkHttpClient());
    }
    
    InfluxDbHttpClient(String baseUrl, Optional<InfluxDbCredentials> credentials, OkHttpClient httpClient) {
        this.baseUrl = requireNonNull(baseUrl, "baseUrl can't be null");
        this.credentials = credentials;
        this.httpClient = httpClient;
    }

    @Override
    public void writePoints(String database, String retentionPolicy, List<DataPoint> points) {
        // Get the line protocol string for each point and join with newlines.
        String lineProtocolString = points.stream()
                .map(DataPoint::lineProtocolString)
                .collect(Collectors.joining("\n"));

        // Build the URL.
        HttpUrl url = urlBuilder(Endpoint.WRITE)
                .addQueryParameter(QueryParam.DATABASE, database)
                .addQueryParameter(QueryParam.RETENTION_POLICY, retentionPolicy)
                .addQueryParameter(QueryParam.PRECISION, MS_PRECISION)
                .build();
        
        // Build the request.
        Request request = requestBuilder(url)
                .post(RequestBody.create(TEXT_PLAIN, lineProtocolString))
                .build();
        
        LOGGER.debug("InfluxDB write request: {}", request);
        
        // Execute the request.
        Response response;
        try {
            response = httpClient.newCall(request).execute();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
        LOGGER.debug("InfluxDB write response: {}", response);
        
        // The response status code should be 204 (no content). If not, throw an exception.
        if (response.code() != NO_CONTENT_STATUS_CODE) {
            LOGGER.debug("Got invalid status code in response to InfluxDB write request: {}", response.code());
            
            // Get the response body as a string, because it probably contains some info about the error that occurred.
            String responseBodyString = Optional.ofNullable(response.body())
                    .map(InfluxDbHttpClient::getResponseBodyString)
                    .orElse("");
            throw new InfluxDbHttpWriteException(response.code(), responseBodyString);
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
     * Get a response's body as a string, rethrowing any {@link IOException} that occurs as an {@link UncheckedIOException}.
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
