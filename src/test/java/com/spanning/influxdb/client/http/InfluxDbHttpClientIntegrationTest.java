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

import com.spanning.influxdb.client.InfluxDbClient;
import com.spanning.influxdb.client.exception.InfluxDbHttpQueryException;
import com.spanning.influxdb.client.exception.InfluxDbHttpWriteException;
import com.spanning.influxdb.model.DataPoint;
import com.spanning.influxdb.model.Field;
import com.spanning.influxdb.model.QueryResult;
import com.spanning.influxdb.model.Series;
import com.spanning.influxdb.model.Tag;
import com.spanning.influxdb.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class InfluxDbHttpClientIntegrationTest {
    
    private static final String BASE_URL = "http://localhost:8086";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String DB = "test_db";
    private static final InfluxDbClient client = new InfluxDbHttpClient(BASE_URL, USERNAME, PASSWORD);
    
    @Before
    public void recreateDb() {
        // Before running each test, drop and create the database.
        client.executeQuery(DB, String.format("DROP DATABASE %s", DB));
        client.executeQuery(DB, String.format("CREATE DATABASE %s", DB));
    }
    
    @Test
    public void testWriteAndQueryPoint() {
        // Write a single point to InfluxDB. Use a measurement name containing a space and comma to test that the client
        // escapes the name properly.
        String measurementName = "measurementNam, e";
        DataPoint dataPoint = createTestPoint(measurementName);
        client.writePoint(DB, dataPoint);
        
        // Attempt to query for the point.
        List<QueryResult> results = client.executeQuery(DB, String.format("SELECT * FROM /%s/", measurementName));
        
        // Since only one query was performed, there should only be one result in the results.
        assertEquals(1, results.size());
        
        // Since the query was for data points in the dataPoint.measurementName series, only one series should have
        // been returned.
        List<Series> seriesList = results.get(0).getSeries();
        assertEquals(1, seriesList.size());

        // The series name should be measurementName.
        Series series = seriesList.get(0);
        assertEquals(measurementName, series.getName());
        
        // Only one point has been written, so there should only be one list of values in series.values.
        assertEquals(1, series.getValues().size());
        
        // Assert that the data point written was returned in the results.
        assertDataPointInSeries(dataPoint, series);
    }
    
    @Test
    public void testWriteAndQueryPoints() {
        // Create a list of 10 DataPoints (with different measurement names) and write them to InfluxDB. Use
        // measurement names containing spaces and commas to test that the client escapes names properly.
        String measurementNamePrefix = "measurementNam, e";
        List<DataPoint> dataPoints = IntStream.range(0, 10)
                .mapToObj(i -> createTestPoint(measurementNamePrefix + i))
                .collect(Collectors.toList());
        client.writePoints(DB, dataPoints);
        
        // Attempt to query for the points.
        List<QueryResult> results = client.executeQuery(DB, String.format("SELECT * FROM /%s.*/", measurementNamePrefix));

        // Since only one query was performed, there should only be one result in the results.
        assertEquals(1, results.size());
        
        // Since there was one measurement name per data point, dataPoints.size series should have been returned.
        List<Series> seriesList = results.get(0).getSeries();
        assertEquals(dataPoints.size(), seriesList.size());
        
        // There should only be one data point in each series returned.
        seriesList.forEach(series -> assertEquals(1, series.getValues().size()));
        
        // Group the series by name and assert that each expected point was returned.
        Map<String, Series> nameToSeries = seriesList.stream()
                .collect(Collectors.toMap(Series::getName, Function.identity()));
        dataPoints.forEach(dataPoint ->
                assertDataPointInSeries(dataPoint, nameToSeries.get(dataPoint.getMeasurementName())));
    }
    
    @Test(expected = InfluxDbHttpWriteException.class)
    public void testWriteStringToBooleanField() {
        // Write a point that establishes the booleanFieldName field of the series identified by measurementName
        // as a boolean field.
        String booleanFieldName = "booleanField";
        String measurementName = "measurementName";
        client.writePoint(DB, new DataPoint.Builder(measurementName)
                .withField(booleanFieldName, false)
                .build());
        try {
            // Attempt to write a point with a string value for the booleanFieldName field. Since the field already has a
            // type of boolean, this should fail.
            client.writePoint(DB, new DataPoint.Builder(measurementName)
                    .withField(booleanFieldName, "stringValue")
                    .build());
        } catch (InfluxDbHttpWriteException e) {
            // The status code should be 400 (bad request).
            assertEquals(400, e.getStatusCode());
            throw e;
        }
    }
    
    @Test(expected = InfluxDbHttpWriteException.class)
    public void testWritePointToNonExistentDatabase() {
        // Drop the non-existent database, just in case it exists.
        String nonExistentDb = "dne";
        client.executeQuery(nonExistentDb, String.format("DROP DATABASE %s", nonExistentDb));
        try {
            // Attempt to write a point to nonExistentDb. Since the database doesn't exist, this should fail.
            client.writePoint(nonExistentDb, createTestPoint("measurementName"));
        } catch (InfluxDbHttpWriteException e) {
            // The status code should be 404 (not found).
            assertEquals(404, e.getStatusCode());
            throw e;
        }
    }
    
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteUnparsableQuery() {
        try {
            // Attempt to execute a query with invalid syntax.
            client.executeQuery(DB, "this isn't the right syntax at all");
        } catch (InfluxDbHttpQueryException e) {
            // The status code should be 400 (bad request).
            assertEquals(400, e.getStatusCode());
            throw e;
        }
    }
    
    @Test
    public void testExecuteQueryOnNonExistentDatabase() {
        // Drop the non-existent database, just in case it exists.
        String nonExistentDb = "dne";
        client.executeQuery(nonExistentDb, String.format("DROP DATABASE %s", nonExistentDb));

        // Attempt to perform a query against nonExistentDb. When an attempt is made to write a point to a non-existent
        // database, InfluxDB returns a response with a 404 status code, but when attempting to query against a
        // non-existent database, InfluxDB returns a single query result with an error message.
        List<QueryResult> results = client.executeQuery(nonExistentDb, "SELECT * FROM /.*/");
        
        // There should only be one result returned.
        assertEquals(1, results.size());
        
        // The result should contain 0 series and have an error message.
        QueryResult result = results.get(0);
        assertTrue(result.getSeries().isEmpty());
        assertNotNull(result.getError());
    }
    
    @Test(expected = InfluxDbHttpQueryException.class)
    public void testExecuteRequestWithoutCredentials() {
        // Create an InfluxDbHttpClient that doesn't use any auth credentials.
        InfluxDbClient client = new InfluxDbHttpClient(BASE_URL);
        try {
            // Attempt to execute a query without credentials. Since no auth credentials are passed in the request,
            // this should fail. Note that the InfluxDB server being used for these tests must have the http.auth-enabled
            // setting set to true, or this test will fail.
            client.executeQuery(DB, "SELECT * FROM /.*/");
        } catch (InfluxDbHttpQueryException e) {
            // The status code should be 401 (unauthorized).
            assertEquals(401, e.getStatusCode());
            throw e;
        }
        
    }

    /**
     * Assert that a {@link Series} contains an expected {@link DataPoint}.
     * @param expectedPoint A point that is expected to be in the series.
     * @param series A {@link Series}.
     */
    private static void assertDataPointInSeries(DataPoint expectedPoint, Series series) {
        // Convert the columns list and values lists to a list of maps of column name to column value.
        List<Map<String, Object>> pointsFields = series.getValues()
                .stream()
                .map(columnValues -> getPointValuesMap(series.getColumns(), columnValues))
                .collect(Collectors.toList());
        
        // Assert that the series has the expected tags.
        Map<String, String> expectedTags = expectedPoint.getTags()
                .stream()
                .collect(Collectors.toMap(Tag::getName, Tag::getValue));
        assertEquals(expectedTags, series.getTags());
        
        // Assert that at least one point in the series has the expected fields.
        assertTrue(pointsFields.stream()
                .anyMatch(pointFields -> hasExpectedFields(expectedPoint, pointFields)));
    }

    /**
     * Convert a list of column names and column values to a map of column name to column value.
     * @param columnNames A list of column names.
     * @param columnValues A list of column values where the nth element corresponds to the value of the nth column
     * in {@code columnNames}.
     * @return A {@link Map} of column name to column value.
     */
    private static Map<String, Object> getPointValuesMap(List<String> columnNames, List<Object> columnValues) {
        return IntStream.range(0, columnNames.size())
                .boxed()
                .collect(Collectors.toMap(columnNames::get, columnValues::get));
    }

    /**
     * Check if a map of field values from a point in a {@link Series} returned in a {@link QueryResult} has the
     * fields from an expected {@link DataPoint}.
     * @param expectedPoint The expected point.
     * @param pointFields A map of field names to field values for a data point in a {@link Series}.
     * @return true if the point is expected, otherwise false.
     */
    private static boolean hasExpectedFields(DataPoint expectedPoint, Map<String, Object> pointFields) {
        // The timestamp isn't treated as a normal field in DataPoint, so remove it from the pointFields map and
        // check it separately.
        long timestamp = Instant.parse((String) pointFields.remove("time")).toEpochMilli();
        
        // Convert the expected point's tags/fields to maps.
        Map<String, Object> expectedFields = expectedPoint.getFields()
                .stream()
                .collect(Collectors.toMap(Field::getFieldName, Field::getFieldValue));
        
        return expectedPoint.getTimestamp() == timestamp && expectedFields.equals(pointFields);
    }

    /**
     * Create a test {@link DataPoint} with a string, boolean, int, and float field.
     * @return A {@link DataPoint}.
     */
    private static DataPoint createTestPoint(String measurementName) {
        return new DataPoint.Builder(measurementName)
                // Use a string value containing quotes to test that the client escapes fields properly.
                .withField("stringField", "\"stringValue\"")
                .withField("booleanField", true)
                .withField("intField", 1234)
                .withField("floatField", 1234.4321)
                // Use a tag value containing a space and comma to test that the client escapes tags properly.
                .withTag("tagName", ", t,agValue")
                .build();
    }
    
}
