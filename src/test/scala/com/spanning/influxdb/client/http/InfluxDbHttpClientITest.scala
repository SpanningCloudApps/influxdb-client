package com.spanning.influxdb.client.http

import java.util.Optional

import com.fasterxml.jackson.databind.ObjectMapper
import com.spanning.influxdb.client.InfluxDbClient.Credentials
import com.spanning.influxdb.client.exception.{InfluxDbHttpQueryException, InfluxDbHttpWriteException}
import com.spanning.influxdb.model._
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfter, FlatSpec, Inspectors, Matchers}

import scala.collection.JavaConversions._

object InfluxDbHttpClientITest {

  private final val BASE_URL = "http://localhost:8086"
  private final val USER_NAME = "root"
  private final val PASSWORD = "root"
  private final val MEASUREMENT_NAME = "test_measurement"
  private final val EXISTING_DB = "test_db"
  private final val DEFAULT_RP = "default"
  private final val BOOLEAN_FIELD_NAME = "booleanField"

  // Build a point that has string, boolean, int, and float fields.
  private def createTestPoint(measurementName: String = MEASUREMENT_NAME): DataPoint =
    new DataPoint.Builder(measurementName)
      .withField("stringField", "someValue")
      .withField(BOOLEAN_FIELD_NAME, true)
      .withField("intField", 4321)
      .withField("floatField", 12.34)
      .withTag("tagName", "tagValue")
      .build()
  
}

class InfluxDbHttpClientITest extends FlatSpec with Matchers with BeforeAndAfter with Inspectors {

  import InfluxDbHttpClientITest._

  val client = new InfluxDbHttpClient(BASE_URL, Optional.of(new Credentials(USER_NAME, PASSWORD)), new ObjectMapper())

  // Before running each test, drop/recreate EXISTING_DB.
  before({
    client.executeQuery(EXISTING_DB, s"DROP DATABASE $EXISTING_DB")
    client.executeQuery(EXISTING_DB, s"CREATE DATABASE $EXISTING_DB")
  })

  "InfluxDB http client" should "read/write a single point successfully" in {
    // If no exception is thrown, the point was written successfully.
    val testPoint = createTestPoint()
    client.writePoint(EXISTING_DB, DEFAULT_RP, testPoint)
    
    // Attempt to query for the point.
    val queryResults: Seq[QueryResult] = client.executeQuery(EXISTING_DB, s"SELECT * FROM $MEASUREMENT_NAME")
    
    // Assert the query results are expected.
    assertQueryResults(Seq(testPoint), queryResults)
  }

  it should "read/write multiple points successfully" in {
    // Create a list of 10 DataPoints (with different measurement names) and write them to InfluxDB.
    val dataPoints = List.range(0, 10).map(i => createTestPoint(MEASUREMENT_NAME + i))

    // If no exception is thrown, the points were written successfully.
    client.writePoints(EXISTING_DB, DEFAULT_RP, dataPoints)
    
    // Query for the points written.
    val queryResults: Seq[QueryResult] = client.executeQuery(EXISTING_DB, s"SELECT * FROM /$MEASUREMENT_NAME.*/")
    
    // Assert the query results are expected.
    assertQueryResults(dataPoints, queryResults)
  }

  it should "fail to write a point when a boolean is written to an existing field that has a string type" in {
    // Write a point that establishes the test_measurement.booleanField field to have type boolean.
    client.writePoint(EXISTING_DB, DEFAULT_RP, createTestPoint())

    // Attempt to write a point with a string value for BOOLEAN_FIELD_NAME. This should fail since the field is already
    // a boolean.
    intercept[InfluxDbHttpWriteException] {
      client.writePoint(EXISTING_DB, DEFAULT_RP, new DataPoint.Builder(MEASUREMENT_NAME)
        .withField(BOOLEAN_FIELD_NAME, "stringValue")
        .build())
    } match {
      // The status code should be 400 "bad request" since the field has the incorrect type. 
      case e: InfluxDbHttpWriteException => e.getStatusCode should equal(HttpStatus.SC_BAD_REQUEST)
    }
  }

  it should "fail to write a point to a non-existent database" in {
    val nonExistingDb = "doesNotExist1234"
    
    // Drop nonExistingDb (just in case it exists).
    client.executeQuery(nonExistingDb, s"DROP DATABASE $nonExistingDb")
    
    intercept[InfluxDbHttpWriteException] {
      // Since the database doesn't exist, this should throw an InfluxDbHttpWriteException.
      client.writePoint(nonExistingDb, DEFAULT_RP, createTestPoint())
    } match {
      // The status code should be 404 "not found" since the db doesn't exist.
      case e: InfluxDbHttpWriteException => e.getStatusCode should equal(HttpStatus.SC_NOT_FOUND)
    }
  }
  
  it should "fail to query when an unparsable query is used" in {
    intercept[InfluxDbHttpQueryException] {
      // Attempt to query using invalid syntax. This call should throw an InfluxDbHttpQueryException.
      client.executeQuery(EXISTING_DB, "this isn't the right syntax at all")
    } match {
        // The status code should be 400 "bad request" since the query was invalid.
      case e: InfluxDbHttpQueryException => e.getStatusCode should equal(HttpStatus.SC_BAD_REQUEST)
    }
  }
  
  it should "fail to query when a non-existing database is queried" in {
    val nonExistingDb = "doesNotExist4321"
    
    // Drop nonExistingDb (just in case it exists).
    client.executeQuery(nonExistingDb, s"DROP DATABASE $nonExistingDb")

    val queryResults = client.executeQuery(nonExistingDb, "SELECT * FROM /.*/")
    
    // There should only be one result.
    queryResults should have size 1
    
    // The result should have no series and should have an error.
    queryResults.head.getSeries shouldBe empty
    queryResults.head.getError should not be empty
  }
  
  it should "fail to make a request when the incorrect credentials are used" in {
    // Create a client with no credentials.
    val noCredentialsClient = new InfluxDbHttpClient(BASE_URL, Optional.empty(), new ObjectMapper())
    
    intercept[InfluxDbHttpQueryException] {
      // The client isn't using credentials, so calling executeQuery should throw an exception. If this test fails
      // because no exception was thrown, it probably means that InfluxDB has http auth disabled
      noCredentialsClient.executeQuery(EXISTING_DB, "SELECT * FROM /.*/")
    } match {
      // The status code should be 401 "unauthorized".
      case e: InfluxDbHttpQueryException => e.getStatusCode should equal(HttpStatus.SC_UNAUTHORIZED)
    }
  }
  
  // Assert that the results of an InfluxDB query are expected.
  private def assertQueryResults(expectedPoints: Seq[DataPoint], queryResults: Seq[QueryResult]) = {
    // Only one query was performed, so only one result should have been returned.
    queryResults should have size 1

    // There should be dataPoints.size series in the results.
    queryResults.head.getSeries should have size expectedPoints.size

    // Group the expected data points by measurement name.
    val expectedPointsByName: Map[String, Seq[DataPoint]] = expectedPoints.groupBy(_.getMeasurementName)

    // Assert that each series has the expected data points.
    queryResults.head.getSeries.foreach(series => assertExpectedSeries(expectedPointsByName(series.getName), series))
  }

  // Assert that the series returned in an InfluxDB query result has the expected points.
  private def assertExpectedSeries(expectedPoints: Seq[DataPoint], series: Series) = {
    // The number of points in the series should be equal to expectedPoints.size.
    series.getValues should have size expectedPoints.size

    // Assert that the expected tags are on the series.
    val seriesTags = series.getTags
    expectedPoints.head.getTags.foreach(expectedTag => seriesTags(expectedTag.getName) should equal(expectedTag.getValue))

    // Create a map of column name -> column value for each point in the series.
    val seriesColumns: Seq[String] = series.getColumns
    val seriesPoints: Seq[Map[String, Object]] = series.getValues.map(colVals => seriesColumns.zip(colVals).toMap)

    // Assert that all of the expected points can be found in the seriesPoints.
    forAll (expectedPoints) { expectedPoint =>
      seriesPoints.exists(isExpectedPoint(_, expectedPoint)) should be(true)
    }
  }

  // Check if an expected data point matches the field values of one of the points returned by an InfluxDB query.
  private def isExpectedPoint(point: Map[String, Object], expectedPoint: DataPoint): Boolean =
    // If all field values are the same, it's the expected point.
    expectedPoint.getFields.forall(expectedField => point(expectedField.getFieldName) == expectedField.getFieldValue)

}
