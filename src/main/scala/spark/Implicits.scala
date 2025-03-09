package spark

object Implicits {
  final case class Memory(value: MemorySize, unit: ITCapacityUnit) {
    override def toString: String = s"$value$unit"
  }

  type MemorySize     = Int
  type ITCapacityUnit = String // MB, GB, KB

  implicit class IntWithMemorySize(value: MemorySize) {
    // Este implicit permite que cualquier valor del tipo `Int` pueda llamar directamente a `.Gb` o `.Mb`,
    // sin necesidad de transformar explícitamente el valor primero (es decir, `42.Gb` funciona directamente).
    def Gb: Memory = Memory(value, "g")
    def Mb: Memory = Memory(value, "m")
    // ... add more memory units as needed
  }
}

