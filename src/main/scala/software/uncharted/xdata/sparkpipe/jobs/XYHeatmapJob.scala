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
import org.apache.spark.sql.{SQLContext, SparkSession}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.ZXYOp
import software.uncharted.xdata.ops.salt.{MercatorHeatmapOp, CartesianHeatmapOp}
import software.uncharted.xdata.ops.util.DebugOperations
import software.uncharted.xdata.sparkpipe.config.{TilingConfig, XYHeatmapConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, dataframeFromSparkCsv}

/**
  * Simple job to do ordinary 2-d tiling
  */
// scalastyle:off method.length
object XYHeatmapJob extends AbstractJob {
  /**
    * This function actually executes the task the job describes
    *
    * @param sqlc   An SQL context in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(sparkSession: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = XYHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }


    val exists_xyBounds = heatmapConfig.xyBounds match {
      case ara: Some[(Double, Double, Double, Double)] => true
      case None => false
      case _ => logger.error("Invalid XYbounds"); sys.exit(-1)
    }

    val tileSize = tilingConfig.bins.getOrElse(ZXYOp.TILE_SIZE_DEFAULT)

    // create the heatmap operation based on the projection
    val heatmapOperation = heatmapConfig.projection match {
      case Some("mercator") =>
      MercatorHeatmapOp (
          heatmapConfig.xCol,
          heatmapConfig.yCol,
          heatmapConfig.valueCol,
          tilingConfig.levels,
          if (exists_xyBounds) heatmapConfig.xyBounds else None,
          tileSize
        )(_)
      case Some("cartesian") | None =>
        CartesianHeatmapOp(
          heatmapConfig.xCol,
          heatmapConfig.yCol,
          heatmapConfig.valueCol,
          tilingConfig.levels,
          if (exists_xyBounds) heatmapConfig.xyBounds else None,
          tileSize
        )(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    // Pipe the dataframe
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession))
      .to(_.cache)
      .to(heatmapOperation)
      .to(serializeBinArray)
      .to(outputOperation)
      .run()

    // create and save extra level metadata - the tile x,y dimensions in this case
    writeMetadata(config, tileSize)
  }

  private def writeMetadata(baseConfig: Config, binCount: Int): Unit = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val levelMetadata = ("bins" -> binCount)
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))
  }
}
