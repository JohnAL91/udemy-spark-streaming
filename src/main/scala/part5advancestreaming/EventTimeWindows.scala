package part5advancestreaming

import org.apache.spark.sql.SparkSession

object EventTimeWindows {
  /*
    -
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    
  }
}
