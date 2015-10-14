# influxdb-client
A java client for InfluxDB v0.9.2

### Usage
#### Create the client:
```java
Optional<Credentials> credentials = Optional.of(new InfluxDbClient.Credentials("user", "password"));
InfluxDbClient client = new InfluxDbHttpClient("http://localhost:8086", credentials, new ObjectMapper());
```

#### Write data:
```java
// Create the point.
DataPoint point = new DataPoint.Builder("measurementName")
        .withField("someStringField", "someStringValue")
        .withField("someBooleanField", true)
        .withField("someIntField", 1234)
        .withField("someDoubleField", 1234.4321)
        .withTag("tagName", "tagValue")
        .withInstant(Instant.now())
        .build();

// Write the point.
client.writePoint("databaseName", "retentionPolicy", point);
```
Note: You can also write multiple points in a single request using the `InfluxDbClient.writePoints` method.

#### Query for data:
```java
List<QueryResult> results = client.query("databaseName", "SELECT * FROM /.*/");
```