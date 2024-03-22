package com.test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType

object ReadJson {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("KafkaJsonToDF")
      .master("local[*]")
      .getOrCreate()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my-topic")
      .load()
      .selectExpr("CAST(value AS STRING)")

    val flattenedDF = kafkaDF
      .withColumn("eid", get_json_object(col("value"), "$.eid"))
      .withColumn("ets", get_json_object(col("value"), "$.ets").cast(LongType))
      .withColumn("ver", get_json_object(col("value"), "$.ver"))
      .withColumn("mid", get_json_object(col("value"), "$.mid"))
      .withColumn("actor_id", get_json_object(col("value"), "$.actor.id"))
      .withColumn("actor_type", get_json_object(col("value"), "$.actor.type"))
      .withColumn("context_channel", get_json_object(col("value"), "$.context.channel"))
      .withColumn("context_pdata_id", get_json_object(col("value"), "$.context.pdata.id"))
      .withColumn("context_pdata_ver", get_json_object(col("value"), "$.context.pdata.ver"))
      .withColumn("context_pdata_pid", get_json_object(col("value"), "$.context.pdata.pid"))
      .withColumn("context_env", get_json_object(col("value"), "$.context.env"))
      .withColumn("context_sid", get_json_object(col("value"), "$.context.sid"))
      .withColumn("context_did", get_json_object(col("value"), "$.context.did"))
      .withColumn("context_cdata", get_json_object(col("value"), "$.context.cdata"))
      .withColumn("context_rollup", get_json_object(col("value"), "$.context.rollup"))
      .withColumn("object", get_json_object(col("value"), "$.object"))
      .withColumn("tags", get_json_object(col("value"), "$.tags"))
      .withColumn("edata_type", get_json_object(col("value"), "$.edata.type"))
      .withColumn("edata_mode", get_json_object(col("value"), "$.edata.mode"))
      .withColumn("edata_pageid", get_json_object(col("value"), "$.edata.pageid"))
      .withColumn("edata_duration", get_json_object(col("value"), "$.edata.duration").cast(LongType))
      .drop("value")
    flattenedDF.printSchema()

    // Write the flattened DataFrame to a console sink for demonstration
    val query = flattenedDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}