package part2StructuredStreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object StreamingJoins {

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // Static DataFrames
  val guitarPlayers: DataFrame = spark.read.json("src/main/resources/data/guitarPlayers/guitarPlayers.json")
  val guitars: DataFrame = spark.read.json("src/main/resources/data/guitars/guitars.json")
  val bands: DataFrame = spark.read.json("src/main/resources/data/bands/bands.json")

  // joining static DFs
  val joinCondition: Column = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands: DataFrame = guitarPlayers.join(bands, joinCondition, "inner")

  // streamed join
  def joinStreamWithStatic(): Unit = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // A DF with a single column "value" of type String
      .select(from_json($"value", bands.schema) as "band") // Creates a composite column
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // Join happens PER BATCH
    val streamedBandGuitarists = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")

    /*
      Restricted joins:
      - Streaming joining with static: RIGHT outer join / full outer join /right_semi are not permitted
      - Static joining with streaming: LEFT outer join / full outer join /left_semi are not permitted
    */
    streamedBandGuitarists.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since spark 2.3 we have stream vs stream joins
  def joinStreamWithStream(): Unit = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // A DF with a single column "value" of type String
      .select(from_json($"value", bands.schema) as "band") // Creates a composite column
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 54321)
      .load() // A DF with a single column "value" of type String
      .select(from_json($"value", guitarPlayers.schema) as "guitarPlayer") // Creates a composite column
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // Join happens PER BATCH
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"), "inner")

    /*
      - inner joins are supported
      - left / right outer joins are supported, but MUST have watermarks
      - full outer joins are NOT supported
    */
    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // Only append supported for stream vs stream join
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    joinStreamWithStatic()
    joinStreamWithStream()
  }
}
