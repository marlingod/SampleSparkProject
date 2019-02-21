package com.mostval.spark

/**
project Skeleton to build spark work

 **/
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser

import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli._
import org.apache.commons.cli.ParseException



object SampleProject {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    //Command LIne parsing
    val options = new Options()
    options.addOption("h", "help", false, "print this help")
    options.addOption("in", "landing", true, "landing folder")
    options.addOption("ev", "event", true, "events folder")
    //options.addOption("ar", "argument", true, "arguments folder")
    // options.addOption("pr", "profile", true, "profile folder")
    //options.addOption("nt", "network", true, "network folder")
    //options.addOption("re", "register", true, "register folder")

    val parser: CommandLineParser = new BasicParser()
    val cli: CommandLine = parser.parse(options, args)

    if (cli.getOptions.length < 1 || cli.hasOption('h')) {
      val formatter: HelpFormatter = new HelpFormatter
      formatter.printHelp("ami ", options)
    }
    val landing = if (cli.hasOption("in"))   cli.getOptionValue("in")
    val events = if (cli.hasOption("ev"))   cli.getOptionValue("ev")

    val filePath = args(0)
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()


    val df =spark.read.option("header", true)csv(filePath)
    df.show()
    df.count()
  }
}
