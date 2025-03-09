package spark

import config.AppConfig.kafkaBootstrapServers
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp

object DataProcessing {
  // Devuelve un Dataset con una tupla de (valor, timestamp), donde el campo valor es un string
  def getKafkaStream(topic: String, spark: SparkSession): Dataset[(String, Timestamp)] = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
  }
}
