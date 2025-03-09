package spark.datavalidations

import domain.IoTDomain.TemperatureHumidityData
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class FullDataValidationTest extends AnyFunSuite with EitherValues {

  test("validarDatosSensorTemperatureHumidity should parse valid data (with Cats)") {
    val input = "sensor1,23.5,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isRight)
    val expected = TemperatureHumidityData("sensor1", 23.5, 45.2, Timestamp.valueOf("2023-10-05 14:00:00"))
    assert(result.value == expected)
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid sensorId (with Cats)") {
    val input = "invalid-sensor!,23.5,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid sensorId"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid temperature (with Cats)") {
    val input = "sensor1,invalid-temp,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid temperature"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid humidity (with Cats)") {
    val input = "sensor1,23.5,invalid-humidity,2023-10-05 14:00:00"
    val result = DataValidationsWithCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid humidity"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid timestamp (with Cats)") {
    val input = "sensor1,23.5,45.2,invalid-timestamp"
    val result = DataValidationsWithCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid timestamp"))
  }

  test("validarDatosSensorTemperatureHumidity should parse valid data (without Cats)") {
    val input = "sensor1,23.5,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isRight)
    val expected = TemperatureHumidityData("sensor1", 23.5, 45.2, Timestamp.valueOf("2023-10-05 14:00:00"))
    assert(result.value == expected)
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid sensorId (without Cats)") {
    val input = "invalid-sensor!,23.5,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid sensorId"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid temperature (without Cats)") {
    val input = "sensor1,invalid-temp,45.2,2023-10-05 14:00:00"
    val result = DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid temperature"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid humidity (without Cats)") {
    val input = "sensor1,23.5,invalid-humidity,2023-10-05 14:00:00"
    val result = DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid humidity"))
  }

  test("validarDatosSensorTemperatureHumidity should fail for invalid timestamp (without Cats)") {
    val input = "sensor1,23.5,45.2,invalid-timestamp"
    val result = DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity(input)
    assert(result.isLeft)
    assert(result.left.value.contains("Invalid timestamp"))
  }
}