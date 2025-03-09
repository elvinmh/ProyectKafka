package config

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object AppConfig {

  // TODO: ¿Qué tal si usamos el fichero application.conf y TypeSafe Config?
  val config: Config = ConfigFactory.load()
  // Configuración de Kafka
  //val kafkaBootstrapServers = "localhost:9092"
  val kafkaBootstrapServers: String = config.getString("kafka.bootstrap.servers")
  //val temperatureHumidityTopic = "temperature_humidity"
  val temperatureHumidityTopic: String = config.getString("topics.temperature-humidity")

  val co2Topic: String = config.getString("topics.co2")

  val soilMoistureTopic = "soil_moisture"

  // Rutas de directorios
  val rutaBase: String = config.getString("ruta-base") // "./tmp/"
  val sufijoCheckpoint ="_chk"


  object Tablas {
    val RawTemperatureHumidityZone = "raw_temperature_humidity_zone"
    val TemperatureHumidityZoneMerge = "temperature_humidity_zone_merge"
  }

  def getRutaParaTabla(nombreTabla: String): String = {
    rutaBase + nombreTabla
  }

  def getRutaParaTablaChk(nombreTabla: String): String = {

    getRutaParaTabla(nombreTabla) + sufijoCheckpoint
  }

}

object ConfigApp extends App {
  import AppConfig.Tablas._
  import AppConfig._

  // Ver test/scala/config/ConfigTest.scala
  println(getRutaParaTabla(RawTemperatureHumidityZone))
  assert(getRutaParaTabla(RawTemperatureHumidityZone) == "./tmp/raw_temperature_humidity_zone")
  println(getRutaParaTablaChk(RawTemperatureHumidityZone))
  assert(getRutaParaTablaChk(RawTemperatureHumidityZone) == "./tmp/raw_temperature_humidity_zone_chk")

  println(getRutaParaTabla(TemperatureHumidityZoneMerge))
  assert(getRutaParaTabla(TemperatureHumidityZoneMerge) == "./tmp/temperature_humidity_zone_merge")
  println(getRutaParaTablaChk(TemperatureHumidityZoneMerge))
  assert(getRutaParaTablaChk(TemperatureHumidityZoneMerge) == "./tmp/temperature_humidity_zone_merge_chk")
}
