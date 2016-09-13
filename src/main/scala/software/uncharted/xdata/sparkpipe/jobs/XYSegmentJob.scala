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
import software.uncharted.xdata.ops.salt.{CartesianTimeHeatmap, MercatorTimeHeatmap}
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYTimeHeatmapConfig}
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

// scalastyle:off method.length
object XYTSegmentJob extends Logging {

  private val convertedTime: String = "convertedTime" // XXX what's this?

  def execute(config: Config): Unit = {

    // // parse the schema, and exit on any errors
    // val schema = Schema(config).getOrElse {
    //   error("Couldn't create schema - exiting")
    //   sys.exit(-1)
    // }
    //
    // // Parse tiling parameters out of supplied config
    // val tilingConfig = TilingConfig(config).getOrElse {
    //   logger.error("Invalid tiling config")
    //   sys.exit(-1)
    // }
    //
    // // Parse geo heatmap parameters out of supplied config
    // val segmentConfig = XYSegmentConfig(config).getOrElse {
    //   logger.error("Invalid heatmap op config")
    //   sys.exit(-1)
    // }
    //
    // // Parse output parameters and return the correspoding write function
    // val outputOperation = createTileOutputOperation(config).getOrElse {
    //   logger.error("Output operation config")
    //   sys.exit(-1)
    // }
    //
    // case Some("cartesian") | None => CartesianSegmentOp(
    //   segmentConfig.yCol                              xCol: String,
    //   segmentConfig.xCol                              yCol: String,
    //   segmentConfig.timeCol                           rangeCol: String,
    //   None                                            valueCol: Option[String],
    //   None                                            latLonBounds: Option[(Double, Double, Double, Double)],
    //   segmentConfig.timeRange                         timeRange: RangeDescription[Long],
    //   tilingConfig.levels,                            zoomLevels: Seq[Int],
    //   tilingConfig.bins.getOrElse(CartesianTimeHeatmap.defaultTileSize) tileSize: Int = defaultTileSize)
    // )(_)

    // create the heatmap operation based on the projection
    val heatmapOperation = segmentConfig.projection match {
      case Some("cartesian") => CartesianSegmentOp(
        // TODO
        // arcType: ArcTypes.Value,        // scalastyle:ignore
        // minSegLen: Option[Int],
        // maxSegLen: Option[Int],
        // x1Col: String,
        // y1Col: String,
        // x2Col: String,
        // y2Col: String,
        // xyBounds: (Double, Double, Double, Double),
        // zBounds: (Int, Int),
        // valueExtractor: Row => Option[T],
        // binAggregator: Aggregator[T, U, V],
        // tileAggregator: Option[Aggregator[V, W, X]],
        // tileSize: Int)
        // (request: TileRequest[(Int, Int, Int)])
      )(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    // Create the spark context from the supplied config
    val sqlc = SparkConfig(config)
    try {
      // Create the dataframe from the input config
      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)

      // Pipe the dataframe
      Pipe(df)
        // .to(_.select(segmentConfig.xCol, segmentConfig.yCol, segmentConfig.timeCol))
        // .to(_.cache())
        // .to(heatmapOperation)
        // .to(serializeBinArray)
        // .to(outputOperation)
        // .run()

      // create and save extra level metadata - the tile x,y,z dimensions in this case
      writeMetadata(config, tilingConfig, segmentConfig)

    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata(baseConfig: Config, tilingConfig: TilingConfig, segmentConfig: XYSegmentConfig): Unit = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val binCount = tilingConfig.bins.getOrElse(MercatorTimeHeatmap.defaultTileSize)
    val levelMetadata =
      ("bins" -> binCount) ~
      ("range" ->
        (("start" -> segmentConfig.timeRange.min) ~
          ("count" -> segmentConfig.timeRange.count) ~
          ("step" -> segmentConfig.timeRange.step)))

    // TODO

    // val jsonBytes = compactRender(levelMetadata).getBytes.toSeq
    // createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))
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
    XYTSegmentJob.execute(args)
  }
}
