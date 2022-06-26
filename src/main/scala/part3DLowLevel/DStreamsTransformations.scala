package part3DLowLevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformations {

  val spark: SparkSession = SparkSession.builder()
    .appName("DStreamsTransformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople(): DStream[Person] = {
    ssc.socketTextStream("localHost", 12345).map { str =>
      val tokens = str.split(":")
      Person(
        tokens(0).toInt, // id
        tokens(1), // first name
        tokens(2), // middle name
        tokens(3), // last name
        tokens(4), // gender
        Date.valueOf(tokens(5)), // date of birth
        tokens(6), // ssn/uuid
        tokens(7).toInt // salary
      )
    }
  }

  // map
  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  // flatmap
  def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  // filter
  def peopleHighIncome: DStream[Person] = readPeople().filter(_.salary > 80000)

  // count
  def countPeople(): DStream[Long] = readPeople().count() // the number of entries in every batch

  // count by value, PER BATCH. Below counts how many of the same name there are for each batch.
  def countNames(): DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  // reduce by key - only operates on Dstream of tuples in which the key is the first type, and the second is value.
  // works PER BATCH
  def countNamesReduce(): DStream[(String, Int)] = readPeople()
    .map(_.firstName)
    .map(name => (name, 1))
    .reduceByKey((a, b) => a + b)

  // foreachRDD - Below: save the contents of a DStream as something else than a textfile
  def saveToJson() = readPeople().foreachRDD { rdd =>
    if(!rdd.isEmpty()) {
      val ds = spark.createDataset(rdd)
      val f = new File("src/main/resources/data/people")
      val nFiles = f.listFiles().length
      val path = s"src/main/resources/data/people/people$nFiles.json"

      ds.write.json(path)
    }
  }

  def main(args: Array[String]): Unit = {
//    val stream = countNames
//    stream.print()

    saveToJson()

    ssc.start()
    ssc.awaitTermination()
  }
}
