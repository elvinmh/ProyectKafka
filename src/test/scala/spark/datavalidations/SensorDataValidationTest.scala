package spark.datavalidations

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class SensorDataValidationTest extends AnyFunSuite with EitherValues {

  test("extractParts should split valid CSV string into parts") {
    val input = "sensor1,23.5,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithCats.extractParts(input)
    assert(result.isRight)
    assert(result.value.sameElements(Array("sensor1", "23.5", "45.2", "2023-10-05 14:00:00")))
  }

  test("extractParts should fail if field count does not match expected") {
    val input = "sensor1,23.5,45.2"
    val result = DataValidationsWithCats.extractParts(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid input format"))
  }

  test("validateSensorId should validate correctly formatted sensorId") {
    val validSensorId = "sensor_123"
    val result = DataValidationsWithCats.validateSensorId(validSensorId)
    assert(result.isRight)
    assert(result.value == validSensorId)
  }

  test("validateSensorId should fail for invalid sensorId") {
    val invalidSensorId = "sensor#123"
    val result = DataValidationsWithCats.validateSensorId(invalidSensorId)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid sensorId"))
  }

  test("validateTemperature should validate valid temperature") {
    val validTemperature = "23.5"
    val result = DataValidationsWithCats.validateTemperature(validTemperature)
    assert(result.isRight)
    assert(result.value == 23.5)
  }

  test("validateTemperature should fail for invalid temperature") {
    val invalidTemperature = "invalid-temp"
    val result = DataValidationsWithCats.validateTemperature(invalidTemperature)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid temperature"))
  }

  test("validateHumidity should validate valid humidity") {
    val validHumidity = "45.2"
    val result = DataValidationsWithCats.validateHumidity(validHumidity)
    assert(result.isRight)
    assert(result.value == 45.2)
  }

  test("validateHumidity should fail for invalid humidity") {
    val invalidHumidity = "invalid-humidity"
    val result = DataValidationsWithCats.validateHumidity(invalidHumidity)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid humidity"))
  }

  test("validateTimestamp should validate a valid timestamp") {
    val validTimestamp = "2023-10-05 14:00:00"
    val result = DataValidationsWithCats.validateTimestamp(validTimestamp)
    assert(result.isRight)
    assert(result.value.toString == Timestamp.valueOf(validTimestamp).toString)
  }

  test("validateTimestamp should fail for invalid timestamp format") {
    val invalidTimestamp = "invalid-timestamp"
    val result = DataValidationsWithCats.validateTimestamp(invalidTimestamp)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid timestamp"))
  }

  test("validateTimestamp should fail for timestamp out of range") {
    val futureTimestamp = "3023-10-05 14:00:00" // Far in the future
    val result = DataValidationsWithCats.validateTimestamp(futureTimestamp)
    assert(result.isLeft)
    assert(result.left.value.contains("Must be within a valid range"))
  }
}