package org.structured_streaming_sources.examples

import org.apache.spark.sql.SparkSession

/**
  * Created by hluu on 3/19/18.
  */
object DataSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SocketSourceExample")
      .master("local[*]")
      .getOrCreate()

    val fileData = spark.readStream.format("rate").load()


  }
}
