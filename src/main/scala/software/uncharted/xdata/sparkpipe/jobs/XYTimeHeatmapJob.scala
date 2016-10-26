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
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.{CartesianTimeHeatmap, MercatorTimeHeatmap}
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTimeHeatmapConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

// scalastyle:off method.length
object XYTimeHeatmapJob extends AbstractJob {

  private val convertedTime: String = "convertedTime"

  def execute(sqlc: SQLContext, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = XYTimeHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // create the heatmap operation based on the projection
    val heatmapOperation = heatmapConfig.projection match {
      case Some("mercator") => MercatorTimeHeatmap(heatmapConfig.yCol, heatmapConfig.xCol, heatmapConfig.timeCol,
        None, None, heatmapConfig.timeRange, tilingConfig.levels,
        tilingConfig.bins.getOrElse(MercatorTimeHeatmap.defaultTileSize))(_)
      case Some("cartesian") | None => CartesianTimeHeatmap(heatmapConfig.yCol, heatmapConfig.xCol, heatmapConfig.timeCol,
        None, None, heatmapConfig.timeRange, tilingConfig.levels,
        tilingConfig.bins.getOrElse(CartesianTimeHeatmap.defaultTileSize))(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    try {
      // Pipe the dataframe
      Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc))
        .to(_.select(heatmapConfig.xCol, heatmapConfig.yCol, heatmapConfig.timeCol))
        .to(_.cache())
        .to(heatmapOperation)
        .to(serializeBinArray)
        .to(outputOperation)
        .run()

      // create and save extra level metadata - the tile x,y,z dimensions in this case
      writeMetadata(config, tilingConfig, heatmapConfig)
    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, heatmapConfig: XYTimeHeatmapConfig): Unit = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val binCount = tilingConfig.bins.getOrElse(MercatorTimeHeatmap.defaultTileSize)
    val levelMetadata =
      ("bins" -> binCount) ~
      ("range" ->
        (("start" -> heatmapConfig.timeRange.min) ~
          ("count" -> heatmapConfig.timeRange.count) ~
          ("step" -> heatmapConfig.timeRange.step)))
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))
  }
}
