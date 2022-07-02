package part5advancestreaming

import org.apache.spark.sql.SparkSession

object ProcessingTimeWindows {
  /*
    -
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("ProcessingTimeWindows")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    
  }
}
