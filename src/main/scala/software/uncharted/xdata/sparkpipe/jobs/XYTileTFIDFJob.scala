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
import org.apache.spark.sql.SQLContext
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.serializeElementDoubleScore
import software.uncharted.xdata.ops.salt.text.TFIDFWordCloud
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTileTFIDFConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createTileOutputOperation, dataframeFromSparkCsv}

import scala.util.{Failure, Success}

object XYTileTFIDFJob extends AbstractJob {
  // Parse TF/IDF parameters out of supplied config
  private def parseTFIDFConfig (config: Config) = {
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


  /**
    * This function actually executes the task the job describes
    *
    * @param sqlc   An SQL context in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(sqlc: SQLContext, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)
    val tfidfConfig = parseTFIDFConfig(config)

    val TFOperation = createProjection(tfidfConfig, tilingConfig)
    val IDFOperation = TFIDFWordCloud.doTFIDF(tfidfConfig.wordsToKeep)(_)

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
