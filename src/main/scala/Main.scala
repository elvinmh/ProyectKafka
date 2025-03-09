import AppConfig._
import config.AppConfig._
import domain.IoTDomain._
import org.apache.spark.sql.functions.{avg, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.DataProcessing.getKafkaStream
import spark.datavalidations.DataProcessingUtils.processAndSeparateData
import spark.datavalidations.DataValidations._
import spark.datavalidations.DataValidationsWithCats
import spark.datavalidations.ExampleDataAndConfig.inputData
import spark.{DataProcessing, ErrorLevel, SparkSessionWrapper, SparkUtils}
import utils.DataTransformations._
import utils.DirectoryCleaner

object AppConfig {
  // Mapeo de sensores a zonas
  // Ejemplo: sensor1 -> zona1, sensor2 -> zona2
  type SensorId = String
  type ZoneId = String
  val Zone1: ZoneId = "zona1"
  val Zone2: ZoneId = "zona2"
  val Zone3: ZoneId = "zona3"

  // Identifica la zona a la que pertenece un dispositivo
  val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
    "sensor1" -> Zone1,
    "sensor2" -> Zone1,
    "sensor3" -> Zone1,
    "sensor4" -> Zone2,
    "sensor5" -> Zone2,
    "sensor6" -> Zone2,
    "sensor7" -> Zone3,
    "sensor8" -> Zone3,
    "sensor9" -> Zone3)

  val OneMinute = "1 minute"
  val OneHour = "1 hour"
  val OneDay = "1 day"
  val OneWeek = "1 week"
  val OneMonth = "1 month"
  val OneYear = "1 year"

  val WatermarkDuration: String = OneMinute
  val WindowDuration: String = OneMinute

}

object Main extends SparkUtils with SparkSessionWrapper {
  // Si estamos en modo desarrollo isDevEnvironment = true se borran los directorios de ./tmp
  private val isDevEnvironment = true

  // Configuración de Spark Session
  override implicit val spark: SparkSession = createSparkSession
    .withName("IoT Farm Monitoring")
    .withCheckpointLocation("./tmp/checkpoint")
    .withTunedShufflePartitions(10)
    .withDeltaLakeSupport
    .withLogLevel(ErrorLevel)
    .build

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    if (isDevEnvironment) {
      println("WARNING: Cleaning temp directory")
      DirectoryCleaner.cleanTempDirectory(rutaBase)
    }

    // Leer datos de Kafka para temperatura y humedad

    val temperatureHumidityDF: Dataset[TemperatureHumidityData] = DataProcessing.getKafkaStream(temperatureHumidityTopic, spark).map {
      case (value, timestamp) =>
        //validarDatosSensorTemperatureHumidity(value)
        // Puedes probar esta que no usa Cats
        /*
        val (invalidData, validData) = processAndSeparateData(inputData, DataValidationsWithoutCats.validarDatosSensorTemperatureHumidity)
        validData.headOption.getOrElse(
          throw new IllegalArgumentException("No valid TemperatureHumidityData found.")
        )
        */
        // O esta que si usa Cats
        val (invalidData2, validData2) = processAndSeparateData(inputData, DataValidationsWithCats.validarDatosSensorTemperatureHumidity)
        validData2.headOption.getOrElse(
          throw new IllegalArgumentException("No valid TemperatureHumidityData found.")
        )
    }

    val temperatureHumidityDFWithZone = addZoneColumn(temperatureHumidityDF)

    val schema: StructType = temperatureHumidityDFWithZone.schema
    val emptyDF = createEmptyDataFrame(schema)

    emptyDF.write
      .format("delta")
      // En lugar de: .save("./tmp/raw_temperature_humidity_zone")
      // lo hacemos con una función
      .save(getRutaParaTabla(Tablas.RawTemperatureHumidityZone))

    emptyDF.write
      .format("delta")
      .partitionBy("zoneId", "sensorId")
      .save(getRutaParaTabla(Tablas.TemperatureHumidityZoneMerge))


    temperatureHumidityDFWithZone.writeStream
      .format("delta")
      .option("checkpointLocation", getRutaParaTablaChk(Tablas.RawTemperatureHumidityZone))
      .trigger(Trigger.ProcessingTime("5 second"))
      .start(getRutaParaTabla(Tablas.RawTemperatureHumidityZone))

    spark.readStream
      .format("delta")
      .load(getRutaParaTabla(Tablas.RawTemperatureHumidityZone))
      .coalesce(1)
      .writeStream
      .option("mergeSchema", "true")
      .outputMode("append")
      .partitionBy("zoneId", "sensorId")
      .format("delta")
      .option("checkpointLocation", "./tmp/temperature_humidity_zone_merge_chk")
      .trigger(Trigger.ProcessingTime("60 second"))
      .start("./tmp/temperature_humidity_zone_merge")

    spark.readStream
      .format("delta")
      .load("./tmp/temperature_humidity_zone_merge")
      .coalesce(1)
      .writeStream
      .outputMode("append")
      .format("json")
      .partitionBy("zoneId", "sensorId")
      .start("./tmp/temperature_humidity_zone_merge_json")


    // Procesamiento y agregación de datos en tiempo real (Ejemplo: Promedio de temperatura por minuto)
    val avgTemperatureDF = temperatureHumidityDFWithZone
      .filter($"zoneId" =!= "unknown")
      .withWatermark("timestamp", WatermarkDuration)
      .groupBy(
        window($"timestamp".cast("timestamp"), WindowDuration),
        $"zoneId"
      )
      .agg(avg($"temperature").as("avg_temperature"))

    // Escribir resultados en la consola (puede ser almacenado en otro sistema)
    val query = avgTemperatureDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 second"))
      .start()

    // Mostrar los dispositivos que no están mapeados a una zona
    temperatureHumidityDFWithZone.filter(unknownZoneFilter)
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("20 second"))
      .start()


    val co2DF = getKafkaStream(co2Topic, spark).map {
      case (value, timestamp) =>
        validarDatosSensorCO2(value, timestamp)
    }

    val avgCo2DF = co2DF
      .withWatermark("timestamp", WatermarkDuration)
      .groupBy(
        window($"timestamp".cast("timestamp"), WindowDuration),
        $"sensorId"
      )
      .agg(avg($"co2Level").as("avg_co2Level"))

    avgCo2DF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 second"))
      .start()

    val soilMoistureDF = getKafkaStream(soilMoistureTopic, spark).map {
      case (value, timestamp) =>
        validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
    }
    val avgSoilMoistureDF = soilMoistureDF
      .withWatermark("timestamp", WatermarkDuration)
      .groupBy(
        window($"timestamp".cast("timestamp"), WindowDuration),
        $"sensorId"
      )
      .agg(avg($"soilMoisture").as("avg_soilMoisture"))

    avgSoilMoistureDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 second"))
      .start()

    // Unificar los datos de los diferentes sensores
    //val unifiedData = ???

    query.awaitTermination()

  }

}