package utils

import org.scalatest.funsuite.AnyFunSuite
import utils.DataTransformations._
import domain.IoTDomain._
import spark.SparkSessionWrapper

class DataTransformationsTest extends AnyFunSuite with SparkSessionWrapper {

  import spark.implicits._

  test("addZoneColumn should correctly add zoneId based on sensorId") {
    // Create test input data
    val inputData = Seq(
      TemperatureHumidityData("sensor1", 25.5, 60.5, java.sql.Timestamp.valueOf("2023-10-14 10:15:00")),
      TemperatureHumidityData("sensor4", 22.3, 58.9, java.sql.Timestamp.valueOf("2023-10-14 10:20:00")),
      TemperatureHumidityData("sensor7", 20.8, 50.2, java.sql.Timestamp.valueOf("2023-10-14 10:25:00")),
      TemperatureHumidityData("unknown_sensor", 19.5, 45.0, java.sql.Timestamp.valueOf("2023-10-14 10:30:00"))
    )

    // Convert to DataFrame
    val inputDf = inputData.toDF()

    // Apply transformation
    val resultDf = addZoneColumn(inputDf.as[TemperatureHumidityData])

    // Define expected output
    val expectedData = Seq(
      TemperatureHumidityData("sensor1", 25.5, 60.5, java.sql.Timestamp.valueOf("2023-10-14 10:15:00"), Some("zona1")),
      TemperatureHumidityData("sensor4", 22.3, 58.9, java.sql.Timestamp.valueOf("2023-10-14 10:20:00"), Some("zona2")),
      TemperatureHumidityData("sensor7", 20.8, 50.2, java.sql.Timestamp.valueOf("2023-10-14 10:25:00"), Some("zona3")),
      TemperatureHumidityData("unknown_sensor", 19.5, 45.0, java.sql.Timestamp.valueOf("2023-10-14 10:30:00"), Some("unknown"))
    )

    val expectedDf = expectedData.toDF()

    // Validate schema and contents
    assert(resultDf.schema == expectedDf.schema)
    val resultData = resultDf.as[TemperatureHumidityData].collect()
    val expectedResultData = expectedDf.as[TemperatureHumidityData].collect()
    
    assert(resultData.sameElements(expectedResultData))
  }
}