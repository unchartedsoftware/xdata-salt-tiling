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

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.serializeElementDoubleScore
import software.uncharted.xdata.ops.salt.text.TFIDFWordCloud
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTileTFIDFConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createTileOutputOperation, dataframeFromSparkCsv}

import scala.util.{Failure, Success}

object XYTileTFIDFJob extends Logging {
  // parse the schema, and exit on any errors
  private def getSchema (config: Config) = {
    Schema(config).getOrElse {
      error("Couldn't create schema - exiting")
      sys.exit(-1)
    }
  }

  // Parse tiling parameters out of supplied config
  private def getTilingConfig (config: Config) = {
    TilingConfig(config).getOrElse {
      logger.error("Invalid tiling config")
      sys.exit(-1)
    }
  }

  // Parse TF/IDF parameters out of supplied config
  private def getTFIDFConfig (config: Config) = {
    XYTileTFIDFConfig(config) match {
      case Success(c) => c
      case Failure(e) =>
        logger.error("Error getting TF/IDF config", e)
        sys.exit(-1)
    }
  }

  private def createProjection (tfidfConfig: XYTileTFIDFConfig, tilingConfig: TilingConfig) = {
    tfidfConfig.projection match {
      case "mercator" =>
        TFIDFWordCloud.mercatorTermFrequency(
          tfidfConfig.xColumn, tfidfConfig.yColumn, tfidfConfig.textColumn,
          tilingConfig.levels
        )(_)
      case "cartesian" =>
        if (tfidfConfig.bounds.isEmpty) {
          logger.error("Cartesian projection specified with no bounds")
          sys.exit(-1)
        }
        TFIDFWordCloud.cartesianTermFrequency(
          tfidfConfig.xColumn, tfidfConfig.yColumn, tfidfConfig.textColumn,
          tfidfConfig.bounds.get, tilingConfig.levels
        )(_)
    }
  }

  def execute (config: Config): Unit = {
    config.resolve()

    val schema = getSchema(config)
    val tilingConfig = getTilingConfig(config)
    val tfidfConfig = getTFIDFConfig(config)

    val TFOperation = createProjection(tfidfConfig, tilingConfig)
    val IDFOperation = TFIDFWordCloud.doTFIDF(tfidfConfig.wordsToKeep)(_)

    val outputOperation = createTileOutputOperation(config).getOrElse {
      logger.error("Output operation config")
      sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Process our data
      Pipe(df)
        .to(TFOperation)
        .to(IDFOperation)
        .to(serializeElementDoubleScore)
        .to(outputOperation)
        .run
    }
  }

  def execute(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader()).resolve()
    execute(config)
  }

  def main (args: Array[String]): Unit = {
    execute(args)
  }
}
