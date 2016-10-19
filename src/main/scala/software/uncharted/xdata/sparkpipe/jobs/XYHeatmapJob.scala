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
import software.uncharted.salt.core.projection.numeric.{CartesianProjection, MercatorProjection}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.ZXYOp
import software.uncharted.xdata.sparkpipe.config.{CartesianProjectionConfig, MercatorProjectionConfig, TilingConfig, XYHeatmapConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, dataframeFromSparkCsv}

import scala.util.Success

/**
  * Simple job to do ordinary 2-d tiling
  */
object XYHeatmapJob extends AbstractJob {
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

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = XYHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // create the heatmap operation based on the projection
    val projection = heatmapConfig.projection match {
      case p: Success[MercatorProjectionConfig] =>
        new MercatorProjection(tilingConfig.levels)
      case sp: Success[CartesianProjectionConfig] =>
        val p = sp.get
        new CartesianProjection(tilingConfig.levels, (p.minX, p.minY), (p.maxX, p.maxY))
    }

    val heatmapOperation = ZXYOp(
      projection,
      tilingConfig.bins.getOrElse(ZXYOp.TILE_SIZE_DEFAULT),
      heatmapConfig.xCol,
      heatmapConfig.yCol,
      heatmapConfig.valueCol,
      SumAggregator,
      Some(MinMaxAggregator)
    )(new TileLevelRequest(tilingConfig.levels, (tc: (Int, Int, Int)) => tc._1))(_)

    // Pipe the dataframe
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc))
      .to(_.cache)
      .to(heatmapOperation)
      .to(serializeBinArray)
      .to(outputOperation)
      .run()

    // create and save extra level metadata - the tile x,y dimensions in this case
    writeMetadata(config, tilingConfig, heatmapConfig)
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, heatmapConfig: XYHeatmapConfig): Unit = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val binCount = tilingConfig.bins.getOrElse(ZXYOp.TILE_SIZE_DEFAULT)
    val levelMetadata = ("bins" -> binCount)
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))
  }
}
