/*
 * Copyright (c) 2015 EMC Corporation
 * All Rights Reserved
 */
package com.spanning.influxdb.client;

import com.spanning.influxdb.client.http.InfluxDbHttpClient;
import com.spanning.influxdb.model.DataPoint;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InfluxDbClientTest {

    @Test
    public void testWritePoint() {
        // Mock an InfluxDbHttpClient. We are testing a default method in InfluxDbClient, but mockito doesn't work well
        // with default methods, so just use an InfluxDbHttpClient.
        InfluxDbClient client = mock(InfluxDbHttpClient.class);
        doCallRealMethod().when(client).writePoint(any(), any(), any(DataPoint.class));

        // Verify that writePoint delegates to the implementation's writePoints method.
        String database = "database";
        String retentionPolicy = "retentionPolicy";
        DataPoint dataPoint = mock(DataPoint.class);
        client.writePoint(database, retentionPolicy, dataPoint);
        verify(client, times(1)).writePoints(database, retentionPolicy, Collections.singletonList(dataPoint));
    }
    
}
