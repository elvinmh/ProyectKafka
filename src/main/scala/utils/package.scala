import AppConfig.sensorToZoneMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

package object utils {

  object DataTransformations {
    import domain.IoTDomain._
    val sensorIdToZoneId: UserDefinedFunction = udf((sensorId: String) => sensorToZoneMap.getOrElse(sensorId, "unknown"))

    def addZoneColumn(df: Dataset[TemperatureHumidityData]): DataFrame = {
      df.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))
    }

  }
}
