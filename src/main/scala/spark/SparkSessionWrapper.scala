package spark

import org.apache.spark.sql.SparkSession
import Implicits.Memory
import Implicits.IntWithMemorySize


sealed trait LogLevel {
  def level: String
}

case object ErrorLevel extends LogLevel {
  override def level: String = "ERROR"
}

case object WarnLevel extends LogLevel {
  override def level: String = "WARN"
}

case object InfoLevel extends LogLevel {
  override def level: String = "INFO"
}

case object DebugLevel extends LogLevel {
  override def level: String = "DEBUG"
}

object Log {
  def level(logLevel: LogLevel): Unit = {
    // You can call your 'setLogLevel' function here
    println(s"Setting log level to ${logLevel.level}")
  }
}

trait SparkSessionWrapper {
  implicit val spark: SparkSession = createSparkSession.build

  class SessionBuilder {
    private var driverCores = 1
    private var executorCores = 1
    private var logLevel = "ERROR"
    private var driverMemory: Memory = 1.Gb
    private var executorMemory: Memory = 1.Gb
    private var appName = "spark session"
    private var offHeapEnabled = false
    private var offHeapGbSize: Memory = 0.Gb
    private var withCustomAppName = false
    private var hiveSupportEnabled = false
    private var deltaLakeSupportEnabled = false
    private var shufflePartitionsTuned = false
    private var shufflePartitions: Int = 10
    private var checkPointLocationEnabled = false
    private var checkPointLocation = "./tmp/checkpoint"
    def withLogLevel(level: LogLevel): SessionBuilder = {
      logLevel = level.level
      this
    }

    def withDriverMemory(memory: Memory): SessionBuilder = {
      driverMemory = memory
      this
    }

    def withExecutorMemory(memory: Memory): SessionBuilder = {
      executorMemory = memory
      this
    }

    def withDriverCores(cores: Int): SessionBuilder = {
      driverCores = cores
      this
    }

    def withExecutorCores(cores: Int): SessionBuilder = {
      executorCores = cores
      this
    }

    def withName(name: String): SessionBuilder = {
      withCustomAppName = true
      appName = name
      this
    }

    def withOffHeapEnabled: SessionBuilder = {
      offHeapEnabled = true
      this
    }

    def withOffHeapGbSize(size: Memory): SessionBuilder = {
      offHeapGbSize = size
      this
    }

    def withEnableHiveSupport: SessionBuilder = {
      hiveSupportEnabled = true
      this
    }

    def withDeltaLakeSupport: SessionBuilder = {
      deltaLakeSupportEnabled = true
      this
    }

    def withTunedShufflePartitions(numParts: Int): SessionBuilder = {
      shufflePartitionsTuned = true
      shufflePartitions = numParts
      this
    }

    def withCheckpointLocation(location: String): SessionBuilder = {
      checkPointLocationEnabled = true
      checkPointLocation = location
      this
    }

    private def buildSparkSession(appName: String): SparkSession = {

      var builder: SparkSession.Builder = SparkSession
        .builder()
        .master("local[*]")
        .appName(appName)
        .config("spark.driver.memory", driverMemory.toString)
        .config("spark.executor.memory", executorMemory.toString)
        .config("spark.driver.cores", driverCores.toString)
        .config("spark.executor.cores", executorCores.toString)
        // Desactivamos la web UI de Spark para evitar problemas con los tests
        .config("spark.ui.enabled", "false")
        // Disable all compression for testing purposes
        .config("spark.rdd.compress", "false")                    // Disable RDD compression
        .config("spark.shuffle.compress", "false")                // Disable shuffle compression
        .config("spark.shuffle.spill.compress", "false")          // Disable shuffle spill compression
        .config("spark.broadcast.compress", "false")              // Disable broadcast compression
        .config("spark.sql.parquet.compression.codec", "none")    // Disable Parquet compression
        .config("spark.sql.orc.compression.codec", "none")        // Disable ORC compression
        .config("spark.sql.json.compression.codec", "none")       // Disable JSON compression
        .config("spark.sql.avro.compression.codec", "uncompressed")       // Disable Avro compression
        .config("spark.sql.csv.compression.codec", "none")        // Disable CSV compression)


      if (offHeapEnabled) {
        builder = builder.config("spark.memory.offHeap.enabled", "true")
        builder = builder.config("spark.memory.offHeap.size", offHeapGbSize.toString)
      }

      if (hiveSupportEnabled) {
        builder = builder.enableHiveSupport()
      }
      if (deltaLakeSupportEnabled) {
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      }

      if (shufflePartitionsTuned) {
        builder = builder.config("spark.shuffle.partitions", shufflePartitions)
      }

      if (checkPointLocationEnabled) {
        builder = builder.config("spark.sql.streaming.checkpointLocation", checkPointLocation)
      }

      builder.getOrCreate()
    }

    def build: SparkSession = {
      val session = buildSparkSession("spark session")
      setLogLevel(session, logLevel)
      session
    }

    def build(name: String): SparkSession = {
      val session = buildSparkSession(name)
      setLogLevel(session, logLevel)
      session
    }
  }

  def createSparkSession: SessionBuilder = new SessionBuilder

  private def setLogLevel(spark: SparkSession, logLevel: String): Unit = spark.sparkContext.setLogLevel(logLevel)
}

// Example of usage with DeltaLake support enabled
object SparkSessionWrapperApp extends SparkSessionWrapper with App {

  override implicit val spark: SparkSession = createSparkSession
    .withCheckpointLocation("./tmp")
    .withDeltaLakeSupport
    .build

}

