package com.test

import com.example.SparkInitializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReadJson {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkInitializer.initializeSpark()

    // Initialize StreamingContext with a batch interval of 5 seconds
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    // Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("read-json")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // Extracting the JSON data from the Kafka stream
    val jsonData = stream.map(record => record.value())

    // Define the schema for the JSON data
    val schema: StructType = StructType(Seq(
      StructField("eid", StringType),
      StructField("ets", LongType),
      StructField("ver", StringType),
      StructField("mid", StringType),
      StructField("actor", StructType(Seq(
        StructField("id", StringType),
        StructField("type", StringType)
      ))),
      StructField("context", StructType(Seq(
        StructField("channel", StringType),
        StructField("pdata", StructType(Seq(
          StructField("id", StringType),
          StructField("ver", StringType),
          StructField("pid", StringType)
        ))),
        StructField("env", StringType),
        StructField("sid", StringType),
        StructField("did", StringType),
        StructField("cdata", ArrayType(StringType)),
        StructField("rollup", MapType(StringType, StringType))
      ))),
      StructField("object", MapType(StringType, StringType)),
      StructField("tags", ArrayType(StringType)),
      StructField("edata", StructType(Seq(
        StructField("type", StringType),
        StructField("mode", StringType),
        StructField("pageid", StringType),
        StructField("duration", LongType)
      )))
    ))
    // After extracting the JSON data from the Kafka stream
    jsonData.foreachRDD { rdd =>
      import spark.implicits._

      // Create a DataFrame from JSON data with explicit schema and handling malformed records
      val df = spark.read
        .schema(schema) // Specify the schema explicitly
        .option("mode", "PERMISSIVE") // Handle malformed records
        .json(spark.createDataset(rdd)) // Use json(Dataset[String]) instead of json(RDD[String])

      // Display the DataFrame contents
      df.show()
    }
    streamingContext.start()
    // Wait for the termination
    streamingContext.awaitTermination()
  }
}