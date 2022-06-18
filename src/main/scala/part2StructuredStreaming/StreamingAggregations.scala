package part2StructuredStreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingAggregations {

  val spark: SparkSession = SparkSession.builder()
    .appName("StreamingAggregations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported (this is because spark would need to keep the track of the entire state of the entire stream)

    lineCount.writeStream
      .format("console")
      .outputMode(OutputMode.Complete()) // Append not supported for aggregations without a feature known as watermarks (later in course)
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column) = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select($"value".cast("int").as("number"))
    val aggregationDF = numbers.select(aggFunction($"number").as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete()) // Append not supported for aggregations without a feature known as watermarks (later in course)
      .start()
      .awaitTermination()
  }

  def groupNames() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines
      .select($"value" as "name")
      .groupBy($"name")
      .count()

    names.writeStream
      .format("console")
      .outputMode(OutputMode.Complete()) // Append not supported for aggregations without a feature known as watermarks (later in course)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    streamingCount()
//    numericalAggregations(sum)
//    numericalAggregations(stddev)
    groupNames()
  }
}
