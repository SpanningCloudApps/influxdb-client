# influxdb-client
A java client for InfluxDB v0.9.2

### Usage
#### Create the client:
```java
InfluxDbClient client = new InfluxDbHttpClient("http://localhost:8086");
```
Or, if the InfluxDB server has auth enabled:
```java
InfluxDbClient client = new InfluxDbHttpClient("http://localhost:8086", "username", "password");
```

#### Write data:
```java
// Create the point.
DataPoint point = new DataPoint.Builder("measurementName")
        .withField("someStringField", "someStringValue")
        .withField("someBooleanField", true)
        .withField("someIntField", 1234)
        .withField("someFloatField", 1234.4321)
        .withTag("tagName", "tagValue")
        .withTimestamp(1444940098741L, TimestampPrecision.MILLISECONDS)
        .build();

// Write the point.
client.writePoint("databaseName", "retentionPolicy", point);
```
This will result in the following request being made:
```
--> POST /write?db=databaseName&rp=retentionPolicy&precision=ms HTTP/1.1
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
Content-Type: text/plain; charset=utf-8
Content-Length: 145
Host: localhost:8086
Connection: Keep-Alive
Accept-Encoding: gzip
User-Agent: okhttp/2.5.0

measurementName,tagName=tagValue someStringField="someStringValue",someBooleanField=true,someIntField=1234,someFloatField=1234.4321 1444940098741
--> END POST (145-byte body)
```
If the write is executed successfully, the following response will be returned by the InfluxDB server:
```
<-- HTTP/1.1 204 No Content (7ms)
Content-Encoding: gzip
Request-Id: c37c2801-7381-11e5-8012-000000000000
X-Influxdb-Version: 0.9.2
Date: Thu, 15 Oct 2015 21:14:47 GMT
OkHttp-Selected-Protocol: http/1.1
OkHttp-Sent-Millis: 1444943687231
OkHttp-Received-Millis: 1444943687238
<-- END HTTP (0-byte body)
```
Note: You can also write multiple points in a single request using the `InfluxDbClient.writePoints` method.

#### Query:
```java
List<QueryResult> results = client.executeQuery("databaseName", "SELECT * FROM measurementName");
```
This will result in the following request being made:
```
--> GET /query?db=databaseName&q=select%20*%20from%20measurementName HTTP/1.1
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
Host: localhost:8086
Connection: Keep-Alive
Accept-Encoding: gzip
User-Agent: okhttp/2.5.0
--> END GET
```
If the query is executed successfully, a response with the following body will be returned by the InfluxDB server
(example is of response containing point written in the write example):
```json
{
    "results": [
        {
            "series": [
                {
                    "columns": [
                        "time",
                        "someBooleanField",
                        "someFloatField",
                        "someIntField",
                        "someStringField"
                    ],
                    "name": "measurementName",
                    "tags": {
                        "tagName": "tagValue"
                    },
                    "values": [
                        [
                            "2015-10-15T20:14:58.741Z",
                            true,
                            1234.4321,
                            1234,
                            "someStringValue"
                        ]
                    ]
                }
            ]
        }
    ]
}
```
For more information about the format of JSON returned by InfluxDB, see
https://influxdb.com/docs/v0.9/guides/querying_data.html#querying-data-using-the-http-api.
