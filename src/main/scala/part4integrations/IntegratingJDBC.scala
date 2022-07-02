package part4integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object IntegratingJDBC {
  /*
    - You can't read streams from JDBC (JDBC works with transactions)
    - You can't write to JDBC in a streaming fashion (JDBC works with transactions)
    - But.. you can write batches. New technique - for each batch.
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("IntegratingJDBC")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame since it's a batch - no new data will come from the stream

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars") // Will create the table if it doesnt exist
          .mode(SaveMode.Overwrite)
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }
}
