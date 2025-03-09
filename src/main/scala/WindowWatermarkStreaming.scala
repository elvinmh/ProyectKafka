package meters

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object WindowWatermarkStreaming extends App {
// Creamos o configuramos el SparkSession
val spark = SparkSession.builder
  .appName("WindowWatermarkExample")
  .master("local[*]") // Cambiar para cluster en producción
  .getOrCreate()

import spark.implicits._

// Simulamos un DataStream con datos de eventos (ejemplo ficticio en lugar de Kafka o Socket)
val eventStream = spark.readStream
  .format("rate") // Fuente de datos para simular eventos
  .option("rowsPerSecond", 100) // Producción de 100 eventos por segundo
  .load()
  .select(
    $"value".as("eventId"), // Usamos el valor como identificador ficticio
    ($"timestamp" - expr("INTERVAL 5 SECONDS")).as("eventTime") // Simulamos que los eventos llegan retrasados 5 segundos
  )

// Mostramos el esquema del flujo simulado
println("DATA STREAM SIMULADO")
eventStream.printSchema()

/*
 * Procesamos el flujo aplicando ventanas y watermark.
 * - Watermark para esperar eventos retrasados hasta 10 segundos.
 * - Ventanas de 1 minuto para agrupar los datos por tiempo.
 */
val result = eventStream
  .withWatermark("eventTime", "10 seconds") // Tolerancia a retrasos de hasta 10 segundos
  .groupBy(
    window($"eventTime", "1 minute", "30 seconds") // Ventanas de 1 minuto con deslizamiento de 30 segundos
  )
  .agg(
    count("eventId").as("eventCount") // Contamos la cantidad de eventos en cada ventana
  )
  .select($"window.start".as("windowStart"), $"window.end".as("windowEnd"), $"eventCount")

// Configuramos la salida del streaming
val query = result.writeStream
  .outputMode("append") // Solo agregaremos nuevas filas
  .format("console") // Mostramos los resultados en la consola
  .option("truncate", "false") // Para ver el contenido completo en las filas
  .trigger(Trigger.ProcessingTime("30 seconds")) // Procesar cada 30 segundos
  .start()

// Mantiene la aplicación activa hasta que se detenga manualmente
query.awaitTermination()
}
/*
---

### **Explicación del código**

1. **Fuente de datos simulada:**
   - Se utiliza el formato `rate` de Spark, que genera un flujo constante de datos para simular eventos.
   - Cada fila tiene un atributo llamado `timestamp` que simula la llegada del evento en tiempo real.
   - Para simular retrasos, se resta 5 segundos a la marca de tiempo del evento creando un nuevo campo `eventTime`.

2. **Aplicación de `Watermark`:**
   ```scala
   .withWatermark("eventTime", "10 seconds")
   ```
   - La watermark establece una tolerancia de 10 segundos para eventos retrasados. Los datos con `eventTime` superior a 10 segundos respecto al límite de la ventana serán descartados como "fuera de tiempo".

3. **Aplicación de ventanas (`window`):**
   ```scala
   window($"eventTime", "1 minute", "30 seconds")
   ```
   - Se agrupan los datos del stream en **ventanas de 1 minuto** de duración.
   - Se define un **intervalo deslizante de 30 segundos**, lo que significa que las ventanas se superponen (cada 30 segundos se inicia una nueva ventana).

   Ejemplo de ventanas generadas:
   - `00:00:00 → 00:01:00`
   - `00:00:30 → 00:01:30`
   - `00:01:00 → 00:02:00`, etc.

4. **Agregación (`count`):**
   ```scala
   agg(count("eventId").as("eventCount"))
   ```
   - Se cuenta la cantidad de eventos (`eventId`) que ocurrieron dentro de cada ventana.

5. **Resultados:**
   - Se muestra la salida en formato de consola con las columnas:
     - `windowStart`: El inicio del rango de la ventana.
     - `windowEnd`: El final del rango de la ventana.
     - `eventCount`: El número de eventos en esa ventana.

6. **Salida del stream:**
   ```scala
   .outputMode("append")
   .format("console")
   .trigger(Trigger.ProcessingTime("5 seconds"))
   .start()
   ```
   - `outputMode("append")`: Solo incluye nuevas filas en el resultado.
   - `format("console")`: Muestra los datos en la consola.
   - `Trigger.ProcessingTime("5 seconds")`: Procesa los datos cada 5 segundos.

---

### **Ejemplo de salida esperada en consola**

Si el flujo genera eventos regularmente y se agrupan en ventanas de 1 minuto:

```text
+-----------------------+-----------------------+----------+
| windowStart           | windowEnd            | eventCount |
+-----------------------+-----------------------+----------+
| 2023-10-26 00:00:00  | 2023-10-26 00:01:00  | 600       |
| 2023-10-26 00:00:30  | 2023-10-26 00:01:30  | 300       |
| 2023-10-26 00:01:00  | 2023-10-26 00:02:00  | 400       |
+-----------------------+-----------------------+----------+
```

---

### **Conclusión**

Este ejercicio muestra cómo configurar y trabajar con ventanas y marcas de agua en Spark Structured Streaming. La clave está en:
1. **Watermark**: Define hasta cuánto tiempo aceptarás eventos retrasados.
2. **Window**: Los eventos se agrupan en intervalos de tiempo especificados.
3. **Agregación**: Procesas los datos según una operación, como `count`, `avg`, etc.
*/

