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
import org.apache.spark.sql.SparkSession
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.geometry.IPProjection
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.IPHeatmapOp
import software.uncharted.xdata.sparkpipe.config.IPHeatmapConfig
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.dataframeFromSparkCsv

/**
  * A basic job to do standard IP tiling
  */
object IPHeatmapJob extends AbstractJob {
  /**
    * This function actually executes the task the job describes
    *
    * @param sparkSession   A SparkSession from which to extract input data as a DataFrame
    * @param config The job configuration
    */
  override def execute(sparkSession: SparkSession, config: Config): Unit = {
    config.resolve

    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse IP tiling parameters out of supplied config
    val ipConfig = IPHeatmapConfig(config).recover { case err: Exception =>
      logger.error("Invalid heatmap op config", err)
      sys.exit(-1)
    }.get

    // Create the dataframe from the input config
    val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession)

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
