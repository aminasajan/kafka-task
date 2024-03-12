package com.example

import org.apache.spark.sql.SparkSession

object SparkInitializer {
  def initializeSpark(): SparkSession = {
    SparkSession.builder()
      .appName("taxi trip")
      .master("local[*]")
      .getOrCreate()
  }
}

