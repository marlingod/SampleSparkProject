package com.mostval.spark

/**
project Skeleton to build spark work

 **/
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SampleProject {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val df =spark.read.option("header", true)csv(filePath)
    df.show()
  }
}
