package part4integrations

import common.carsSchema
import org.apache.spark.sql.functions.{expr, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IntegratingKafka {

  val spark: SparkSession = SparkSession.builder()
    .appName("DStreamsTransformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm") // read from the topics (kafka)
      .load()

    kafkaDF.select($"topic", expr("cast(value as string) as actualValue")).writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value") // Needs a "key" and a "value" (must have those names) for kafka to work

    // The "option("checkpointLocation", "checkpoints")" - Important for kafka caching. Without it, the writing will fail. WARNING: To rerun the application you need to delete the directory checkpoints,
    // otherwise new data will not be written to kafka! The checkpoint directory helps keep track of which data has been inputted if spark fails (you don't know where you ended)
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  /**
    * Write the whole cars data structures to kafka as JSON
    * Use struct columns and the to_json function.
    * */
  def writeCarsToKafka(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      $"name" as "key",
      to_json(struct($"Name", $"Horsepower", $"Origin")).cast("String") as "value"
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromKafka()
//    writeToKafka()
    writeCarsToKafka()
  }
}
