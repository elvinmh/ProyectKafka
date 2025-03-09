package kafkagen

import config.AppConfig._
import org.apache.kafka.clients.producer._

import java.sql.Timestamp
import java.util.Properties


object NoModificar {
  // Esta parte desgraciadamente no la podemos modificar porque simula lo que nos envía los dispositivos en su formato
  // Sí que podemos en tiempo de desarrollo probar a enviar datos mal formateados para ver cómo los podríamos gestionar

  private val props = new Properties()
  props.put("bootstrap.servers", kafkaBootstrapServers)
  props.put("key.serializer"  , "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  private def sendData(topic: String, sensorId: String, value: Double, timestamp: Timestamp): Unit = {
    // message definition based on topic
    val message = topic match {
      case "temperature_humidity" => s"$sensorId,$value,$value,$timestamp"
      // Ejemplo de envío de datos que no vienen con el tipo que esperamos:
      // case "co2" => s"$sensorId,${value.toString},$timestamp"
      // case "co2" => s"$sensorId, ,$timestamp"
      // case "co2" => s",${value.toString},$timestamp"
      case "co2" => s"$sensorId,$value,$timestamp"
      case "soil_moisture" => s"$sensorId,$value,$timestamp"
      case _ => throw new Exception("Invalid topic")
    }
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending data: $message to topic $topic")
    producer.send(record)
  }

  def generateAndSendData(topic: String, sensorId: String): Unit = {
    val timestamp = new Timestamp(System.currentTimeMillis())
    sendData(topic, sensorId, Math.random() * 100, timestamp)
  }

  def close(): Unit = {
    producer.close()
  }
}

object KafkaDataGenerator {

  def main(args: Array[String]): Unit = {
    val maxRecords = 1000000
    val maxDevices = 9
    val sleepMs = 10 // Miliseconds for the app to wait between each message
    for (j <- 1 to maxRecords) {
        for (i <- 1 to maxDevices) {
          // Cada 500 registros, se envía un mensaje con un sensor desconocido
          if (j % 50 == 0 && i == 3) {
            NoModificar.generateAndSendData("temperature_humidity", s"sensor-chungo-$i")
            NoModificar.generateAndSendData("co2", s"sensor-chungo-$i")
            NoModificar.generateAndSendData("soil_moisture", s"sensor-chungo-$i")

          } else {
            NoModificar.generateAndSendData("temperature_humidity", s"sensor$i")
            NoModificar.generateAndSendData("co2", s"sensor$i")
            NoModificar.generateAndSendData("soil_moisture", s"sensor$i")
            // Prueba a descomentar esta línea y ver qué pasa:
            //NoModificar.generateAndSendData("soil_moisture_with_adds", s"sensor$i")
            Thread.sleep(sleepMs) // delay for the demo purpose
          }
        }
    }
    NoModificar.close()
  }
}
