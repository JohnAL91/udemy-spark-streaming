package part5advancestreaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object EventTimeWindows {
  /*
    - The moment when the record was GENERATED, set by the data generation system, usually a column in the dataset
    - Different from processing time = the time the record arrives at spark

    Window functions
    - Aggregations on time-based groups
    - Essential concepts:
     - window durations
     - window sliding intervals

    As opposed to DStreams
    - Records are not necessarily taken between "now" and a certain past date
    - we can control output modes

    Window duration and sliding interval must be a multiple of the batch interval
    Output mode will influence results
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val onlinePurchaseSchema: StructType = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType),
  ))

  def readPurchasesFromSocket(): DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json($"value", onlinePurchaseSchema) as "purchase")
    .selectExpr("purchase.*")

  def readPurchasesFromFile(): DataFrame = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  // Aggregate to total quantity of items that we sell per a one day window updated every hour
  def aggregatePurchasesBySlidingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window($"time", "1 day", "1 hour").as("time")) // Struct field: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        $"time".getField("start").as("start"),
        $"time".getField("end").as("end"),
        $"totalQuantity")
      .orderBy("start")

    windowByDay.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesByTumblingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window($"time", "1 day").as("time")) // window($"time", "1 day") as an alternative to the sliding window is called a tumbling window where windowDuration = slideDuration, meaning no overlap at all
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        $"time".getField("start").as("start"),
        $"time".getField("end").as("end"),
        $"totalQuantity")
      .orderBy("start")

    windowByDay.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }


  /**
    * EXERCISES
    * 1) Show the best selling product of everyday + what quantity was sold
    * 2) Show the best selling product of every 24 hours, updated every hour
    */

  def exercise1(): Unit = {
    val purchaseDF = readPurchasesFromFile()

    val windowByDay = window($"time", "1 day").as("time")

    val res = purchaseDF
      .groupBy(windowByDay, $"item")
      .agg(sum($"quantity").as("totalQuantity"))
      .select(
        $"time".getField("start").as("start"),
        $"time".getField("end").as("end"),
        $"item",
        $"totalQuantity")
      .orderBy($"time", $"totalQuantity".desc)

    res.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def exercise2(): Unit = {
    val purchaseDF = readPurchasesFromFile()

    val windowByDay = window($"time", "1 day", "1 hour").as("time")

    val res = purchaseDF
      .groupBy(windowByDay, $"item")
      .agg(sum($"quantity").as("totalQuantity"))
      .select(
        $"time".getField("start").as("start"),
        $"time".getField("end").as("end"),
        $"item",
        $"totalQuantity")
      .orderBy($"start", $"totalQuantity".desc)

    res.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  /*
    For window functions, windows start at Jan 1 1970, 12 AM GMT
  */

  // Requires to run netcat in console..
  def main(args: Array[String]): Unit = {
//    aggregatePurchasesBySlidingWindow()
//    aggregatePurchasesByTumblingWindow()
//    exercise1()
    exercise2()
  }
}
