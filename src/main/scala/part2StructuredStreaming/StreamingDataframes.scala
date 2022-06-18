package part2StructuredStreaming

import common.stocksSchema
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

object StreamingDataframes {

  /*
    Lazy evaluation
    Transformations and Actions
    * Transformations describe of how new DFs are obtained
    * Actions start executing/running Spark code

    Input sources
    * Kafka, Flume
    * Databases
    * Sockets

    Output sinks
    * A distributed file system
    * Databases
    * Kafka
    * testing sinks e.g. console, memory

    Output modes
    * append = only add new records
    * update = modify records in place <- if query has no aggregations, equivalent with append
    * complete = rewrite everything

    Not all queries and sinks support all output modes
    * example: aggregations with append mode

    Triggers = when new data is written
    * default: write as soon as the current micro-batch has been processed
    * once: write a single micro-batch and stop
    * processing-time: look for new data at fixed intervals
    * continuous (currently experimental)
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("StreamingDataFrames")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def readFromSocket() = {
    // Reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformations
    val shortLines: DataFrame = lines.filter(length($"value") <= 5) // Same API as batch (almost) :D

    // tell between a static vs a streaming DF
    println("shortLines.isStreaming: " + shortLines.isStreaming)

    // Consuming a DF
    val query: StreamingQuery = shortLines.writeStream
      .format("console")
      .outputMode(OutputMode.Append()) // The default output mode
      .start() // Async

    // We need to wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema) // Must pass in a schema!
      .load("src/main/resources/data/stocks") // Spark will monitor all the files in the folder

    stocksDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Write the lines DF at a certain trigger. The default trigger value is ProcessingTime(0) and it will run the query as fast as possible.
    lines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(
//        Trigger.ProcessingTime(2.seconds) // Every 2 seconds, run the query.
//        Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every two seconds create a batch with whatever you have (even if you do not have anything new incoming)
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromSocket()
//    readFromFiles()
    demoTriggers()
  }

}
