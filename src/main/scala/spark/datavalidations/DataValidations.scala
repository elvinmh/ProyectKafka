package spark.datavalidations

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.sql.Timestamp
import scala.util.{Success, Try}

object DataValidations {

  import domain.IoTDomain._

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  val unknownZoneFilter: Column = col("zoneId") === "unknown"

  // Function to validate Temperature and Humidity sensor data
  def validarDatosSensorTemperatureHumidity(value: String): TemperatureHumidityData = {
    val parts = value.split(",")

    // Log and return an error if unexpected input structure
    if (parts.length != 4) {
      logger.error(s"Invalid input format: '$value'. Expected 4 parts, found ${parts.length}.")
      throw new IllegalArgumentException(s"Invalid input format: $value")
    }

    val sensorId = parts(0)
    val maybeTemperature = Try(parts(1).toDouble)
    val maybeHumidity = Try(parts(2).toDouble)
    val maybeTimestamp = Try(Timestamp.valueOf(parts(3)))

    // Validate sensorId using a regex
    val sensorIdRegex = """^[a-zA-Z0-9-_]+$""".r
    if (sensorId.isEmpty || sensorIdRegex.findFirstIn(sensorId).isEmpty) {
      logger.error(s"Invalid sensorId: '$sensorId'. Must be alphanumeric and can include '-' or '_'.")
      throw new IllegalArgumentException(s"Invalid sensorId: $sensorId")
    }

    (maybeTemperature, maybeHumidity, maybeTimestamp) match {
      case (Success(temperature), Success(humidity), Success(timestamp)) =>
        // Validate timestamp is in a correct range
        if (timestamp.getTime <= 0 || timestamp.getTime > System.currentTimeMillis()) {
          logger.error(s"Invalid timestamp: '$timestamp'.")
          throw new IllegalArgumentException(s"Invalid timestamp: $timestamp")
        }
        // Return valid data
        TemperatureHumidityData(sensorId, temperature, humidity, timestamp)

      // Handle other invalid inputs
      case _ =>
        logger.error(s"Invalid values for record: '$value'.")
        throw new IllegalArgumentException(s"Invalid values: $value")
    }
  }


  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String, timestamp: Timestamp): SoilMoistureData = {
    // TODO: Implementar validaciones
    // TODO: Revisar si el valor de retorno es correcto (¿qué pasa si el valor no es correcto?)
    val parts = value.split(",")
    logger.debug("validarDatosSensorTemperatureHumiditySoilMoisture: " + parts.length)
    // Hago una validación provisional MUY DEFENSIVA ya que si el assert falla, la aplicación se cerrará, por lo que hay que implementar otro mecanismo
    assert(parts.length == 3)
    SoilMoistureData(parts(0), parts(1).toDouble, timestamp)
  }

  def validarDatosSensorCO2(value: String, timestamp: Timestamp): CO2Data = {
    // TODO: Implementar validaciones
    // TODO: Revisar si el valor de retorno es correcto (¿qué pasa si el valor no es correcto?) -> Ver Option, Try, Either
    val parts = value.split(",")
    logger.debug("validarDatosSensorCO2: " + parts.length)
    // Hago una validación provisional MUY DEFENSIVA ya que si el assert falla, la aplicación se cerrará, por lo que hay que implementar otro mecanismo
    assert(parts.length == 3)
    CO2Data(parts(0), parts(1).toDouble, timestamp)
  }
}
