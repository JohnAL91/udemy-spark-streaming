package part5advancestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object ProcessingTimeWindows {
  /*
    - Processing Time - Time that the records arrive to Spark

    As you see below, we work the exact time as with the Event Time... We just use a different timestamp.
    Spark doesn't care what the time column means.
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("ProcessingTimeWindows")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def aggregateByProcessingTime(): Unit = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12345)
      .load()
      .select($"value", current_timestamp().as("processingTime"))
      .groupBy(window($"processingTime", "10 seconds"))
      .agg(sum(length($"value")) as "count") // counting characters every 10 seconds by processing time
      .select(
        $"window".getField("start") as "start",
        $"window".getField("end") as "end",
        $"count"
      )
    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }
}
