package part4integrations

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object IntegratingKafkaDStreams {
  val spark: SparkSession = SparkSession.builder()
    .appName("DStreamsTransformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092", // To add more servers "localhost:9092, anotherServer:1234"
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receive data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  ) // The serializers that will allow spark and kafka to communicate via binary

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,

      /*
      Distribute the partitions evenly across the Spark cluster.
      Alternatives:
      - PreferBrokers if the brokers and executors are in the same cluster
      - PreferFixed.
      */
      LocationStrategies.PreferConsistent,

      /*
      Alternatives:
      - SubscribePattern - allows subscribing to topics matching a pattern
      - Assign - advanced; allows specifying offsets and partitions per topic.
      */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1")) // Group id association
    )

    val processedStream: DStream[(String, String)] = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)
    val processData = inputData.map(_.toUpperCase())

    processData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // Inside this lambda, the code is run by a single executor

        val kafkaHasMap = new util.HashMap[String, Object]() // Populate the hashmap with all the params from kafkaParams
        kafkaParams.foreach { pair =>
          kafkaHasMap.put(pair._1, pair._2)
        }

        // producer can insert records into the Kafka topics
        // Available on this executor
        val producer = new KafkaProducer[String, String](kafkaHasMap)

        partition.foreach { value => // value is a record in the partition
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)

          // feed the message into the Kafka topic
          producer.send(message)
          producer.close()
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /*
    Remember to:
    * Create the producer per partition as producers are not serializable
    * Create a kafka message per each record
    * close the producer when you're done processing the partition

  */

  def main(args: Array[String]): Unit = {
//    readFromKafka()
    writeToKafka()
  }
}
