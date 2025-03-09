package spark

import domain.IoTDomain.TemperatureHumidityData

import scala.util.matching.Regex

package object datavalidations {

  // Expected format for sensorId
  val sensorIdRegex: Regex = """^[a-zA-Z0-9-_]+$""".r

  // For TemperatureHumidity only
  val ExpectedFieldCount = 4

  private type RawSensorData = String
  type ListOfRawSensorData = Seq[RawSensorData]
  type ValidatedSensorData = Either[String, TemperatureHumidityData]
}
