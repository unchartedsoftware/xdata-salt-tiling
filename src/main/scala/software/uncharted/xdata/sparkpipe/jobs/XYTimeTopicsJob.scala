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
import software.uncharted.sparkpipe.ops.core.dataframe.text.{includeTermFilter, split}
import software.uncharted.xdata.ops.io.serializeElementScore
import software.uncharted.xdata.ops.salt.{CartesianTimeTopics, MercatorTimeHeatmap, MercatorTimeTopics}
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTimeTopicsConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

// scalastyle:off method.length
object XYTimeTopicsJob extends Logging {

  private val convertedTime = "convertedTime"

  def execute(config: Config): Unit = {

    config.resolve()

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
    val topicsConfig = XYTimeTopicsConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // when time format is used, need to pick up the converted time column
    val topicsOp = topicsConfig.projection match {
      case Some("mercator") => MercatorTimeTopics(topicsConfig.yCol, topicsConfig.xCol, topicsConfig.timeCol, topicsConfig.textCol,
        None, topicsConfig.timeRange, topicsConfig.topicLimit, tilingConfig.levels)(_)
      case Some("cartesian") | None => CartesianTimeTopics(topicsConfig.yCol, topicsConfig.xCol, topicsConfig.timeCol, topicsConfig.textCol,
        None, topicsConfig.timeRange, topicsConfig.topicLimit, tilingConfig.levels)(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    val outputOperation = createTileOutputOperation(config).getOrElse {
      logger.error("Output operation config")
      sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        .to(split(topicsConfig.textCol, "\\b+"))
        .to(includeTermFilter(topicsConfig.textCol, topicsConfig.termList.keySet))
        .to(_.select(topicsConfig.xCol, topicsConfig.yCol, topicsConfig.timeCol, topicsConfig.textCol))
        .to(_.cache())
        .to(topicsOp)
        .to(serializeElementScore)
        .to(outputOperation)
        .run()

      writeMetadata(config, tilingConfig, topicsConfig)

    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, topicsConfig: XYTimeTopicsConfig): Unit = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val outputOp = createMetadataOutputOperation(baseConfig)

    val binCount = tilingConfig.bins.getOrElse(MercatorTimeHeatmap.defaultTileSize)
    val levelMetadata =
      ("bins" -> binCount) ~
        ("range" ->
          (("start" -> topicsConfig.timeRange.min) ~
            ("count" -> topicsConfig.timeRange.count) ~
            ("step" -> topicsConfig.timeRange.step)))
    val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    outputOp.foreach(_("metadata.json", jsonBytes))

    val termJsonBytes = compactRender(topicsConfig.termList).toString().getBytes.toSeq
    outputOp.foreach(_("terms.json", termJsonBytes))
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
    XYTimeTopicsJob.execute(args)
  }
}
