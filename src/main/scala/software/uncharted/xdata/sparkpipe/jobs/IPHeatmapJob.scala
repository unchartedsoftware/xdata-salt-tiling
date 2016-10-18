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

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.geometry.IPProjection
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.IPHeatmapOp
import software.uncharted.xdata.sparkpipe.config.{IPHeatmapConfig, Schema, TilingConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}


/**
  * A basic job to do standard IP tiling
  */
class IPHeatmapJob extends AbstractJob {
  /**
    * This function actually executes the task the job describes
    *
    * @param sqlc   An SQL context in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(sqlc: SQLContext, config: Config): Unit = {
    config.resolve

    // parse the schema, and exit on any errors
    val schema = Schema(config).getOrElse {
      error("Couldn't create schema - exiting")
      sys.exit(-1)
    }

    // Parse tiling parameters out of supplied config
    val tilingConfig = TilingConfig(config).getOrElse {
      logger.error("Invalid tiling config")
      sys.exit(-1)
    }

    // Parse IP tiling parameters out of supplied config
    val ipConfig = IPHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // Parse output parameters out of supplied config
    val outputOperation = createTileOutputOperation(config).getOrElse {
      logger.error("Output operation config")
      sys.exit(-1)
    }



    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

    val tilingOp = IPHeatmapOp(new IPProjection(tilingConfig.levels),
      tilingConfig.bins.getOrElse(IPHeatmapOp.defaultTileSize),
      ipConfig.ipCol, ipConfig.valueCol,
      SumAggregator,
      Some(MinMaxAggregator)
    )(new TileLevelRequest(tilingConfig.levels, (tc: (Int, Int, Int)) => tc._1))(_)

    // Pipe the dataframe
    Pipe(df)
      .to(_.cache())
      .to(tilingOp)
      .to(serializeBinArray)
      .to(outputOperation)
      .run()
  }
}
