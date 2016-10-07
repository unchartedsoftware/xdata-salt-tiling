/**
  * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.xdata.sparkpipe.jobs



import java.io.File

import scala.io.Source
import scala.collection.JavaConverters._ // scalastyle:ignore
import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.sql.SQLContext

import software.uncharted.xdata.sparkpipe.config.SparkConfig



/**
  * A basic job trait that standardizes reading and combining config files and execution
  */
trait AbstractJob extends Logging {
  var debug = false
  /**
    * This function actually executes the task the job describes
    *
    * @param sqlc An SQL context in which to run spark processes in our job
    * @param config The job configuration
    */
  def execute(sqlc: SQLContext, config: Config): Unit

  def execute(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val environmentalConfig = ConfigFactory.load()
    val config =
      args.flatMap { cfgFileName =>
        if (cfgFileName == "debug") {
          debug = true
          None
        } else {
          val cfgFile = new File(cfgFileName)
          if (!cfgFile.exists()) {
            logger.warn(s"Config file $cfgFileName doesn't exist")
            None
          } else if (!cfgFile.isFile) {
            logger.warn(s"Config file $cfgFileName is a directory, not a file")
            None
          } else if (!cfgFile.canRead) {
            logger.warn(s"Can't read config file $cfgFileName")
            None
          } else {
            if (debug) {
              println(s"Reading config file $cfgFile")
            }
            Some(ConfigFactory.parseReader(Source.fromFile(cfgFile).bufferedReader()))
          }
        }
      }.fold(environmentalConfig)((base, fallback) => base.withFallback(fallback))

    if (debug) {
      debugConfig(config)
    }

    val sqlc = SparkConfig(config)
    try {
      execute(sqlc, config)
    } finally {
      sqlc.sparkContext.stop()
    }
  }

  def debugConfig (config: Config): Unit = {
    println("Full config info:")
    config.entrySet().asScala.foreach{entry =>
      val key = entry.getKey
      val value = entry.getValue
      println(s"""\t"$key":"$value"""")
    }
  }
  /**
    * All jobs need a main function to allow them to run
    */
  def main (args: Array[String]): Unit = {
    execute(args)
  }
}
