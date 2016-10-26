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

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.{CartesianSegmentOp, MercatorSegmentOp}
import software.uncharted.xdata.sparkpipe.config.{Schema, SparkConfig, TilingConfig, XYSegmentConfig}
import software.uncharted.xdata.ops.io.serializeBinArray
import software.uncharted.xdata.sparkpipe.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation, dataframeFromSparkCsv}

import scala.util.parsing.json.JSONObject

/**
  * Executes the a segment job configured given a path to a configuration file
  *
  * Takes a csv input of pickup points [e.g (lat,lon)] and dropoff points and draws a line between them
  *
  * Outputs tiles in binary format to specified output directory using z/x/y format
  *
  * Refer to resources/xysegment/xysegment.conf for an example configuration file of all the options available
  */
// scalastyle:off method.length
object XYSegmentJob extends AbstractJob {
  def execute(sqlc: SQLContext, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val segmentConfig = XYSegmentConfig(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    // create the segment operation based on the projection
    val segmentOperation = segmentConfig.projection match {
      case Some("cartesian") => CartesianSegmentOp(
        segmentConfig.arcType,
        segmentConfig.minSegLen,
        segmentConfig.maxSegLen,
        segmentConfig.x1Col,
        segmentConfig.y1Col,
        segmentConfig.x2Col,
        segmentConfig.y2Col,
        None,
        segmentConfig.xyBounds,
        tilingConfig.levels,
        segmentConfig.tileSize,
        tms = tilingConfig.tms)(_)
      case Some("mercator") => MercatorSegmentOp(
        segmentConfig.minSegLen,
        segmentConfig.maxSegLen,
        segmentConfig.x1Col,
        segmentConfig.y1Col,
        segmentConfig.x2Col,
        segmentConfig.y2Col,
        None,
        segmentConfig.xyBounds,
        tilingConfig.levels,
        segmentConfig.tileSize,
        tms = tilingConfig.tms)(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    try {
      // Pipe the dataframe
      Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc))
        .to(_.select(segmentConfig.x1Col, segmentConfig.y1Col, segmentConfig.x2Col, segmentConfig.y2Col))
        .to(_.cache())
        .to(segmentOperation)
        .to(writeMetadata(config))
        .to(serializeBinArray)
        .to(outputOperation)
        .run()
    } finally {
      sqlc.sparkContext.stop()
    }
  }

  private def writeMetadata[BC, V](baseConfig: Config)
                                  (tiles: RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]]):
  RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]] = {
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore
    import net.liftweb.json.JsonAST._ // scalastyle:ignore

    val metadata = tiles
      .map(tile => {
        val (level, minMax) = (tile.coords._1, tile.tileMeta.getOrElse((0.0, 0.0)))
        level.toString -> (("min" -> minMax._1) ~ ("max" -> minMax._2))
      })
      .collect()
      .toMap


    val jsonBytes = compactRender(metadata).getBytes.toSeq
    createMetadataOutputOperation(baseConfig).foreach(_("metadata.json", jsonBytes))

    tiles
  }
}
