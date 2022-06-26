package part3DLowLevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStream {
  /*
    DStreams - Discretized Streams
    - DStreams are a never ending sequence of RDDs
    - Spark uses micro batches, and each batch is an RDD
    - Batches are triggered at the same time in the cluster (nodes' clocks are synchronized)

    A DStream needs to be coupled with a receiver (one per DStream)
    - A receiver fetches data from the source, sends to spark, creates blocks.
    - Is managed by the StreamingContext on the driver
    - Occupies one core on the machine
    - Receiver doesn't need to be managed by us (unless we create a custom one)
  */

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  // Entry point to DStream API. The duration = the batch interval
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
  /*
    - Define input sources by creating DStreams
    - Define transformations or DStreams
    - call an action on DStreams
    - start ALL computation with ssc.start()
      - no more computations can be added after
    - await termination, or stop the computation
      - you cannot restart the ssc
  */

//  import spark.implicits._

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation (lazy until action)
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    // wordsStream.print()
    // Each folder = RDD = Batch, each file = a partition of the RDD. Writes to file
    wordsStream.saveAsTextFiles("src/main/resources/data/words/words")

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where we will store new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,May 1 2001,9.98
          |AAPL,Jun 1 2001,11.62
          |AAPL,Jul 1 2001,9.4
          |AAPL,Aug 1 2001,9.27
          |AAPL,Sep 1 2001,7.76
          |AAPL,Oct 1 2001,8.78
          |AAPL,Nov 1 2001,10.65
          |AAPL,Dec 1 2001,10.95
          |AAPL,Jan 1 2002,12.36
          |AAPL,Feb 1 2002,10.85
          |AAPL,Mar 1 2002,11.84
          |""".stripMargin.trim)
      writer.close()
    }).start()
  }

  def readFromFile(): Unit = {
    createNewFile() // Async - writes a file after 5 sec. This is just for testing purposes.

    /*
      ssc.textFileStream() monitors a directory for NEW FILES
   */
    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // Transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    stocksStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
//    readFromFile()
  }
}
