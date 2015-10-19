/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client;

import com.spanning.influxdb.client.http.InfluxDbHttpClient;
import com.spanning.influxdb.model.DataPoint;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InfluxDbClientTest {
    
    private InfluxDbClient client;
    
    @Before
    public void setupClient() {
        // Mock an InfluxDbHttpClient. The tests in this class are testing the default methods in InfluxDbClient, but
        // mockito doesn't work well with default methods, so just use an InfluxDbHttpClient.
        client = mock(InfluxDbHttpClient.class);
        
        // Mock the client to call real default methods.
        doCallRealMethod().when(client).writePoint(any(), any(DataPoint.class));
        doCallRealMethod().when(client).writePoint(any(), any(), any(DataPoint.class));
        doCallRealMethod().when(client).writePoints(any(), anyListOf(DataPoint.class));
        doCallRealMethod().when(client).writePoints(any(), anyString(), anyListOf(DataPoint.class));
    }

    @Test
    public void testWritePoint() {
        // Verify that writePoint(String, DataPoint) delegates to the implementation's
        // writePoints(String, Optional, List) method.
        String database = "database";
        DataPoint dataPoint = mock(DataPoint.class);
        client.writePoint(database, dataPoint);
        verify(client, times(1)).writePoints(database, Optional.empty(), Collections.singletonList(dataPoint));
    }

    @Test
    public void testWritePointWithRetentionPolicy() {
        // Verify that writePoint(String, String, DataPoint) delegates to the implementation's
        // writePoints(String, Optional, List) method.
        String database = "database";
        String retentionPolicy = "retentionPolicy";
        DataPoint dataPoint = mock(DataPoint.class);
        client.writePoint(database, retentionPolicy, dataPoint);
        verify(client, times(1)).writePoints(database, Optional.of(retentionPolicy), Collections.singletonList(dataPoint));
    }
    
    @Test
    public void testWritePoints() {
        // Verify that writePoints(String, List) delegates to the implementation's writePoints(String, Optional, List)
        // method.
        String database = "database";
        List<DataPoint> dataPoints = Stream.generate(() -> mock(DataPoint.class)).limit(5).collect(Collectors.toList());
        client.writePoints(database, dataPoints);
        verify(client, times(1)).writePoints(database, Optional.empty(), dataPoints);
    }
    
    @Test
    public void testWritePointsWithRetentionPolicy() {
        // Verify that writePoints(String, String, List) delegates to the implementation's
        // writePoints(String, Optional, List) method.
        String database = "database";
        String retentionPolicy = "retentionPolicy";
        List<DataPoint> dataPoints = Stream.generate(() -> mock(DataPoint.class)).limit(5).collect(Collectors.toList());
        client.writePoints(database, retentionPolicy, dataPoints);
        verify(client, times(1)).writePoints(database, Optional.of(retentionPolicy), dataPoints);
    }
    
}
