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
import org.apache.spark.sql.DataFrame
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.MercatorTimeHeatmap
import software.uncharted.xdata.sparkpipe.config.{MercatorTimeHeatmapConfig, Schema, SparkConfig, TilingConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

// scalastyle:off method.length
object MercatorTimeHeatmapJob extends Logging {

  private val convertedTime: String = "convertedTime"

  def execute(config: Config): Unit = {
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

    // Parse geo heatmap parameters out of supplied config
    val heatmapConfig = MercatorTimeHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // Parse output parameters and return the correspoding write function
    val outputOperation = createTileOutputOperation(config).getOrElse {
      logger.error("Output operation config")
      sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // when time format is used, need to pick up the converted time column
      val finalTimeCol = heatmapConfig.timeFormat.map(p => convertedTime).getOrElse(heatmapConfig.timeCol)

      // Pipe the dataframe
      Pipe(df)
        .to {
          heatmapConfig.timeFormat
            .map(tf => parseDate(heatmapConfig.timeCol, convertedTime, tf)(_))
            .getOrElse((df: DataFrame) => df)
        }
        .to(_.select(heatmapConfig.lonCol, heatmapConfig.latCol, finalTimeCol))
        .to(_.cache())
        .to {
          MercatorTimeHeatmap(
            heatmapConfig.latCol,
            heatmapConfig.lonCol,
            finalTimeCol,
            None,
            None,
            heatmapConfig.timeRange,
            tilingConfig.levels,
            tilingConfig.bins.getOrElse(MercatorTimeHeatmap.defaultTileSize))
        }
        .to(serializeBinArray)
        .to(outputOperation)
        .run()

      // create and save extra level metadata - the tile x,y,z dimensions in this case
      writeMetadata(config, tilingConfig, heatmapConfig)

    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, heatmapConfig: MercatorTimeHeatmapConfig): Unit = {
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

  def execute(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length < 1) {
      logger.error("Path to conf file required")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader())
    execute(config)
  }

  def main (args: Array[String]): Unit = {
    MercatorTimeHeatmapJob.execute(args)
  }
}
