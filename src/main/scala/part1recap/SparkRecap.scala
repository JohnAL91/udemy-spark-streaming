package part1recap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object SparkRecap extends {

  // the entry point to the Spark structured API
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars")

  import spark.implicits._

  // select
  val usefulCarsData: DataFrame = cars.select(
    col("Name"), // column object
    $"Year", // another column object (needs spark implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWeights: DataFrame = cars.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCars: Dataset[Row] = cars.where(col("Origin") =!= "USA")

  // aggregations
  val averageHP: DataFrame = cars.select(avg(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max

  // grouping
  val countByOrigin: DataFrame = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristsBands: DataFrame = guitarPlayers.join(bands, guitarPlayers.col("band") === bands.col("id"))
  /*
    join types
    - inner: only the matching rows are kept
    - left/right/full outer join
    - semi/anti
   */

  // datasets = typed distributed collection of objects
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  val guitarPlayersDS: Dataset[GuitarPlayer] = guitarPlayers.as[GuitarPlayer] // needs spark.implicits
  guitarPlayersDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars: DataFrame = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin
  )

  // low-level API: RDDs
  val sc: SparkContext = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  val doubles: RDD[Int] = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF: DataFrame = numbersRDD.toDF("number") // you lose type info, you get SQL capability

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)

  // DS -> RDD
  val guitarPlayersRDD: RDD[GuitarPlayer] = guitarPlayersDS.rdd

  // DF -> RDD
  val carsRDD: RDD[Row] = cars.rdd // RDD[Row]

  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    cars.show()
    cars.printSchema()


  }
}
