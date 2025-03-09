package spark.datavalidations

import domain.IoTDomain.{CO2Data, SoilMoistureData, TemperatureHumidityData}

// Trait que encapsula las validaciones comunes para diferentes tipos de datos de sensores
trait SensorDataValidation {
  import cats.implicits._

  import java.sql.Timestamp
  import scala.util.Try

  // Logger compartido para registrar errores o eventos relevantes
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  // Número esperado de campos en el CSV de entrada
  private val ExpectedFieldCount: Int = 4

  // Mensaje de error para datos en formatos no válidos
  val InvalidFormatError: (String, Int) => String = (value, count) =>
    s"Invalid input format: '$value'. Expected $ExpectedFieldCount fields but found $count."

  // Método para dividir los datos en sus partes constituyentes utilizando "," como delimitador
  def extractParts(value: String): Either[String, Array[String]] = {
    val parts = value.split(",") // Dividimos el String en un arreglo
    if (parts.length != ExpectedFieldCount)
      Left(InvalidFormatError(value, parts.length)) // Error si el número de campos no coincide
    else
      Right(parts) // Retornamos las partes si son válidas
  }

  // Validación del identificador del sensor (sensorId)
  // Debe ser alfanumérico y puede incluir "-" o "_"
  def validateSensorId(sensorId: String): Either[String, String] = {
    val sensorIdRegex = "^[a-zA-Z0-9_-]+$".r // Expresión regular para validar el formato
    if (sensorId.nonEmpty && sensorIdRegex.matches(sensorId))
      Right(sensorId) // Retornamos el sensorId si es válido
    else {
      val errorMsg = s"Invalid sensorId: '$sensorId'. Must be alphanumeric and can include '-' or '_'."
      logger.error(errorMsg) // Registramos un error
      Left(errorMsg) // Retornamos un mensaje de error si no cumple
    }
  }

  // Validación de la temperatura, que debe ser un número válido
  def validateTemperature(tempStr: String): Either[String, Double] =
    Try(tempStr.toDouble).toEither // Intentamos convertir el String en Double
      .leftMap(_ => s"Invalid temperature: '$tempStr'. Must be a number.") // Error si no es un número válido

  // Validación de la humedad, que debe ser un número válido
  def validateHumidity(humidityStr: String): Either[String, Double] =
    Try(humidityStr.toDouble).toEither // Intentamos convertir el String en Double
      .leftMap(_ => s"Invalid humidity: '$humidityStr'. Must be a number.") // Error si no es un número válido

  // Validación del timestamp, que debe cumplir con las restricciones de formato y rango válidos
  def validateTimestamp(timestampStr: String): Either[String, Timestamp] =
    Try(Timestamp.valueOf(timestampStr)).toEither // Intentamos convertir el String en un objeto Timestamp
      .leftMap(_ => s"Invalid timestamp: '$timestampStr'. Must be a valid timestamp.") // Error en caso de formato inválido
      .flatMap { timestamp =>
        // Comprobamos si el timestamp está dentro de un rango válido
        if (timestamp.getTime > 0 && timestamp.getTime < System.currentTimeMillis())
          Right(timestamp) // Timestamp válido
        else
          Left(s"Invalid timestamp: '$timestampStr'. Must be within a valid range.") // Fuera de rango
      }
}

// Objeto que utiliza Cats para realizar las validaciones de datos de temperatura y humedad
object DataValidationsWithCats extends SensorDataValidation {
import cats.implicits._
  // Método principal que valida los datos de temperatura y humedad de un sensor
  def validarDatosSensorTemperatureHumidity(value: String): Either[String, TemperatureHumidityData] = {
    extractParts(value).flatMap { parts =>
      (
        validateSensorId(parts(0)),      // Validación del sensorId (primera parte)
        validateTemperature(parts(1)),  // Validación de la temperatura (segunda parte)
        validateHumidity(parts(2)),     // Validación de la humedad (tercera parte)
        validateTimestamp(parts(3))     // Validación del timestamp (cuarta parte)
      ).mapN((id, temp, hum, ts) => TemperatureHumidityData(id, temp, hum, ts, None))
      // Combinamos todos los valores validados para producir un objeto TemperatureHumidityData
    }
  }

  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String): Either[String, SoilMoistureData] = ???
  def validarDatosSensorCO2(value: String): Either[String, CO2Data] = ???

}

// Objeto que realiza las mismas validaciones pero sin utilizar Cats
object DataValidationsWithoutCats extends SensorDataValidation {

  // Método principal que valida los datos de temperatura y humedad de un sensor
  def validarDatosSensorTemperatureHumidity(value: String): Either[String, TemperatureHumidityData] = {
    extractParts(value).flatMap { parts =>
      for {
        id <- validateSensorId(parts(0))      // Validación del sensorId (primera parte)
        temp <- validateTemperature(parts(1)) // Validación de la temperatura (segunda parte)
        hum <- validateHumidity(parts(2))     // Validación de la humedad (tercera parte)
        ts <- validateTimestamp(parts(3))     // Validación del timestamp (cuarta parte)
      } yield TemperatureHumidityData(id, temp, hum, ts, None)
      // Combinamos todos los valores validados para producir un objeto TemperatureHumidityData
    }
  }

  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String): Either[String, SoilMoistureData] = ???
  def validarDatosSensorCO2(value: String): Either[String, CO2Data] = ???

}


object ExampleDataAndConfig {

  val inputData: ListOfRawSensorData = Seq(
    "sensor1,23.5,45.2,2023-10-01 12:30:00", // Valid input
    "sensor-chungo-3,99.0,50.1,2023-10-05 14:00:00", // Valid input
    "invalid-sensor,,45.2,2023-10-01 12:30:00", // Missing temperature
    "sensor-invalid,22.3,-,-", // Invalid format
    "sensor3,23.5,45.2,2025-10-01 12:30:00" // Timestamp in the future
  )
}

object DataProcessingUtils {

  /** Generic function to process and separate input data into valid and invalid categories */
  def processAndSeparateData(
                              input: ListOfRawSensorData,
                              validationFunction: String => ValidatedSensorData
                            ): (ListOfRawSensorData, Seq[TemperatureHumidityData]) = {
    input.map(validationFunction).partitionMap(identity)
  }
}

object DataValidationsExampleWithCatsApp extends App {

  import DataProcessingUtils._
  import ExampleDataAndConfig._

  // Process data using Cats-based validation
  private val (invalidData, validData) = processAndSeparateData(inputData, DataValidationsWithCats.validarDatosSensorTemperatureHumidity)

  // Print valid data
  println("\nValid Data:")
  validData.foreach(data => println(s"      : $data"))

  // Print invalid data
  println("\nInvalid Data:")
  invalidData.foreach(error => println(s"      : $error"))
}

object DataValidationsExampleAppWithoutCats extends App {

  import DataProcessingUtils._
  import ExampleDataAndConfig._

  // Process data using non-Cats validation
  private val (invalidData, validData) = processAndSeparateData(inputData, DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity)

  // Print valid data
  println("\nValid Data:")
  validData.foreach(data => println(s"      : $data"))

  // Print invalid data
  println("\nInvalid Data:")
  invalidData.foreach(error => println(s"      : $error"))
}