package part2StructuredStreaming

import common._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StreamingDatasets {

  val spark: SparkSession = SparkSession.builder()
    .appName("StreamingDatasets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._ // Includes encoders for DF -> DS transformations

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car] // encoder can also be manually given

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load() // DF with single col "value" as string
      .select(from_json($"value", carsSchema) as "car") // Composite column (struct)
      .selectExpr("car.*") // DF with multiple cols
      .as[Car](carEncoder) // A Dataset
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations
    val carNamesDF: DataFrame = carsDS.select($"name")
    val carNamesDS: Dataset[String] = carsDS.map(_.Name)

    carNamesDS.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()

  }

  /**
    * Exercises:
    *
    * 1. Count how many POWERFUL Cars we have in the DS (HP > 140)
    * 2. Average HP for the entire dataset (use the complete output mode)
    * 3. Count the cars by origin
    */

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  def ex2() = {
    val carsDS = readCars()

    // THIS WILL BE THE AVERAGE FOR THE ENTIRE DATA (ALL ROWS) FROM THE START OF THE STREAM BECAUSE WE USE THE COMPLETE OUTPUTMODE
    carsDS.select(avg($"Horsepower"))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  def ex3() = {
    val carsDS = readCars()
    carsDS.groupByKey(_.Origin).count()
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    ex1()
//    ex2()
    ex3()
  }
}
