package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._

object IntegratingCassandra {

  // Will show 2 different techniques for integrating spark with cassandra, one using ForeachWriter (but it is not cassandra specific)

  val spark: SparkSession = SparkSession.builder()
    .appName("IntegratingCassandra")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def writeStreamToCassandraInBatches(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // Will save this batch to cassandra in a single table write
        batch
          .select($"Name", $"Horsepower")
          .write
          .cassandraFormat("cars", "public") // Type enrichment
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  def writeStreamToCassandra(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    class CarCassandraForeachWriter extends ForeachWriter[Car] { // ForeachWriter is not Cassandra specific
      /*
        - on every batch, on every partition `partitionID`
          - on every "epoch" = chunk of data
            - call the open method; if false, skip this chunk
            - for each entry in this chunk, call the process method
            - call the close method at the end of the chunk or with an error if it was thrown
      */

      val keyspace = "public"
      val table = "cars"
      val connector: CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

      override def open(partitionId: Long, epochId: Long): Boolean = {
        println("Open connection")
        true
      }

      override def process(car: Car): Unit = {
        connector.withSessionDo { session =>
          session.execute(
            s"""
               |insert into $keyspace.$table("Name", "Horsepower")
               |values ('${car.Name}', ${car.Horsepower.orNull})
               |""".stripMargin)
        }
      }

      override def close(errorOrNull: Throwable): Unit = println("Closing connection")
    }

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    writeStreamToCassandraInBatches()
    writeStreamToCassandra()
  }
}
