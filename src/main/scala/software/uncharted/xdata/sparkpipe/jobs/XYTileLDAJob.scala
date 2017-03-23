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

import scala.util.{Failure, Success}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.serializeElementDoubleScore
import software.uncharted.xdata.ops.salt.text.{DictionaryConfigurationParser, LDAOp, TextOperations}
import software.uncharted.xdata.sparkpipe.config.{LDAConfig, TileTopicConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.dataframeFromSparkCsv

/**
  * A job that takes tsv data, breaks out a document from each entry as a word bag, tiles the documents into
  * tile-based word bags, and runs Latent Dirichlet Allocation on those tile word bags
  */
object XYTileLDAJob extends AbstractJob {
  // Parse tile topic parameters out of supplied config
  private def parseTileTopicConfig (config: Config) = {
    TileTopicConfig.parse(config) match {
      case Success(c) => c
      case Failure(e) =>
        logger.error("Error getting topic tiling configuration", e)
        sys.exit(-1)
    }
  }

  // Get LDA-specific configuration
  private def parseLDAConfig (config: Config) = {
    LDAConfig.parse(config) match {
      case Success(c) => c
      case Failure(e) =>
        logger.error("Error getting LDA configuration")
        sys.exit(-1)
    }
  }

  // Get dictionary creation configuration
  private def parseDictionaryConfig (config: Config) = {
    DictionaryConfigurationParser.parse(config)
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)
    val tileTopicConfig = parseTileTopicConfig(config)
    val dictionaryConfig = parseDictionaryConfig(config)
    val ldaConfig = parseLDAConfig(config)

    val projection = tileTopicConfig.projectionConfig.createProjection(tilingConfig.levels)
    val wordCloudTileOp = TextOperations.termFrequencyOp(
      tileTopicConfig.xColumn,
      tileTopicConfig.yColumn,
      tileTopicConfig.textColumn,
      projection,
      tilingConfig.levels
    )(_)
    val ldaOperation = LDAOp.ldaWordsByTile[Nothing](dictionaryConfig, ldaConfig)(_)

    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, session)

    // Process our data
    Pipe(df)
      .to(wordCloudTileOp)
      .to(ldaOperation)
      .to(serializeElementDoubleScore)
      .to(outputOperation)
      .run
  }
}
