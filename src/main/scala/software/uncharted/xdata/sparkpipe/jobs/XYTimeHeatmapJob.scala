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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.parseDate
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.ops.salt.{CartesianTimeHeatmap, MercatorTimeHeatmap}
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTimeHeatmapConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

// scalastyle:off method.length
object XYTimeHeatmapJob extends Logging {

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
    val heatmapConfig = XYTimeHeatmapConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // Parse output parameters and return the correspoding write function
    val outputOperation = createTileOutputOperation(config).getOrElse {
      logger.error("Output operation config")
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

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        .to(_.select(heatmapConfig.xCol, heatmapConfig.yCol, heatmapConfig.timeCol))
        .to(_.cache())
        .to(heatmapOperation)
        .to(writeMetadata(config))
        .to(serializeBinArray)
        .to(flipYAxis)
        .to(outputOperation)
        .run()

    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata[BC, V](baseConfig: Config)(tiles: RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]]): RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]] = {
    import net.liftweb.json.JsonDSL._
    // scalastyle:ignore
    import net.liftweb.json.JsonAST._
    // scalastyle:ignore

    val metadata = tiles
      .map(tile => {
        val (level, minMax) = (tile.coords._1, tile.tileMeta.getOrElse((0.0, 0.0)))
        level.toString -> (("min" -> minMax._1) ~ ("max" -> minMax._2))
      })
      .collect()
      .toMap


    val jsonBytes = compactRender(metadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_ ("metadata.json", jsonBytes))

    tiles
  }

  private def flipYAxis(input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    input.map { case (index, data) => {
      val limit = (1 << index._1) - 1
      ((index._1, index._2, limit - index._3), data)
    }
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

  def main(args: Array[String]): Unit = {
    XYTimeHeatmapJob.execute(args)
  }
}
