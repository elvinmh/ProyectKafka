# Explicación de Window y Watermark en Spark Structured Streaming

## 1. Window (Ventanas)

Las ventanas en **Spark Structured Streaming** permiten agrupar los datos que llegan en base a un **rango de tiempo**. Esto es especialmente útil cuando trabajas con datos con marcas de tiempo (eventos) y necesitas analizar o agregar esos eventos en rangos delimitados, como calcular promedios o conteos para intervalos específicos de tiempo.

### ¿Cómo funciona la Window?
- **Enfoque:** La ventana divide los datos entrantes en intervalos de tiempo fijo o deslizantes según la clave de tiempo establecida, normalmente un campo de marca temporal (`timestamp`) en los mensajes.
- **Configuración común:**
  - `Window duration`: Define la duración de cada ventana. Por ejemplo, ventanas de 10 segundos, 1 minuto, etc.
  - `Sliding interval`: (opcional) Define cada cuánto tiempo inicia una ventana nueva en el flujo (esto puede superponerse con ventanas previas dependiendo de su configuración).

### Ejemplo de ventana fija:
Si configuras una ventana de duración de 10 segundos, cada lote de datos se agrupará en intervalos como:
- 10:00:00 → 10:00:09
- 10:00:10 → 10:00:19
- 10:00:20 → 10:00:29, etc.

Esto te permite procesar datos que caen dentro de esas divisiones temporales.

### Ejemplo de ventana deslizante:
Si defines una duración de ventana de 10 segundos, pero con un intervalo deslizante de 5 segundos, entonces obtendrás ventanas como:
- 10:00:00 → 10:00:09
- 10:00:05 → 10:00:14
- 10:00:10 → 10:00:19, etc.

Este tipo de ventanas son útiles para obtener cálculos más frecuentes o superpuestos entre intervalos de tiempo consecutivos.

---

## 2. Watermark (Marca de agua)

Las **watermarks** gestionan el retraso en la llegada de los datos, algo común en sistemas en tiempo real debido a latencias o eventos fuera de orden (cuando los datos no llegan en el orden cronológico correcto).

### ¿Qué hace una Watermark?
La marca de agua establece un límite de tiempo hasta el cual Spark espera por los datos atrasados. Una vez que pasa ese límite, Spark considera que los datos de más allá de ese tiempo **ya no son válidos** para una ventana específica y no los procesará.

- Esto permite a Spark manejar datos atrasados sin consumir recursos de procesamiento indefinidamente.

### Configuración de una Watermark:
  - Al usar el operador `.withWatermark` defines una política para manejar eventos atrasados, especificando cuánto tiempo retrasado puede estar un dato en relación con su marca de tiempo (`event time`).

### Ejemplo:
Supón que estás procesando datos con ventanas de 10 segundos y defines una watermark de 5 segundos con:

```scala
dataset.withWatermark("timestamp", "5 seconds")
```

- Spark esperará hasta 5 segundos adicionales después del límite superior de cada ventana.
- Datos recibidos más tarde que esos 5 segundos **no se procesarán** para esa ventana.

#### Ejemplo detallado:
- Si el intervalo de la ventana es de `09:00:00 → 09:00:10`, Spark podrá considerar datos con timestamps hasta `09:00:15`.
- Si cualquier dato de esa ventana llega después de `09:00:15`, será **descartado** como "fuera de tiempo".

Esto asegura que el procesamiento sea **determinístico** y maneje datos atrasados de manera eficiente.

---

## 3. Cómo funcionan juntos Window y Watermark

Las ventanas permiten agrupar los datos en intervalos de tiempo, mientras que las marcas de agua controlan hasta qué punto Spark aceptará datos atrasados para esas ventanas.

### Ejemplo conjunto:
Supón que quieres calcular el conteo de eventos en ventanas de 1 minuto con tolerancia a retrasos de 1 minuto:

```scala
val result = dataset
  .withWatermark("timestamp", "1 minute")
  .groupBy(window($"timestamp", "1 minute"))
  .count()
```

#### Aquí:
- `window($"timestamp", "1 minute")`: Agrupa los datos en ventanas de 1 minuto.
- `.withWatermark("timestamp", "1 minute")`: Permite que Spark espere hasta 1 minuto adicional por eventos retrasados antes de cerrar la ventana.

### Flujo de procesamiento con watermark:
1. Llega un conjunto de eventos con marcas de tiempo.
2. Cada evento se asigna a una ventana en función de su marca temporal.
3. La watermark define cuántos segundos más esperar para datos atrasados en esa ventana.
4. Una vez pasada la tolerancia (según la watermark), Spark descarta cualquier dato atrasado y considera que la ventana está cerrada.

---

## 4. Conclusión
- **Window**: Segmenta los datos en intervalos definidos para procesarlos en grupos.
- **Watermark**: Define cuánto tiempo esperar por datos retrasados antes de cerrar definitivamente una ventana.
- Ambas trabajan juntas para manejar datos en tiempo real de manera eficiente, incluso cuando hay retrasos o eventos desordenados en el flujo de datos.