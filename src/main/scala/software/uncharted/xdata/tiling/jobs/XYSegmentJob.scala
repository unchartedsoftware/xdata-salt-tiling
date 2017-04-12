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
package software.uncharted.xdata.tiling.jobs

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SparkSession}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.xdata.io.serializeBinArray
import software.uncharted.sparkpipe.ops.xdata.salt.{CartesianSegmentOp, MercatorSegmentOp}
import software.uncharted.xdata.tiling.config.{CartesianProjectionConfig, MercatorProjectionConfig, XYSegmentConfig}
import software.uncharted.xdata.tiling.jobs.JobUtil.{dataframeFromSparkCsv, createMetadataOutputOperation}

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
  // scalastyle:off cyclomatic.complexity
  def execute(sparkSession: SparkSession, config: Config): Unit = {
    val schema = parseSchema(config)
    val tilingConfig = parseTilingParameters(config)
    val outputOperation = parseOutputOperation(config)

    // Parse geo heatmap parameters out of supplied config
    val segmentConfig = XYSegmentConfig.parse(config).getOrElse {
      logger.error("Invalid heatmap op config")
      sys.exit(-1)
    }

    val xyBoundsFound  = segmentConfig.projectionConfig.xyBounds match {
      case ara : Some[(Double, Double, Double, Double)] => true
      case None => false
      case _ => logger.error("Invalid XYbounds"); sys.exit(-1)
    }

    // create the segment operation based on the projection
    val segmentOperation = segmentConfig.projectionConfig match {
      case _: MercatorProjectionConfig => MercatorSegmentOp(
        segmentConfig.minSegLen,
        segmentConfig.maxSegLen,
        segmentConfig.x1Col,
        segmentConfig.y1Col,
        segmentConfig.x2Col,
        segmentConfig.y2Col,
        segmentConfig.valueCol,
        if (xyBoundsFound) segmentConfig.projectionConfig.xyBounds else None,
        tilingConfig.levels,
        segmentConfig.tileSize,
        tms = tilingConfig.tms)(_)
      case _: CartesianProjectionConfig => CartesianSegmentOp(
        segmentConfig.arcType,
        segmentConfig.minSegLen,
        segmentConfig.maxSegLen,
        segmentConfig.x1Col,
        segmentConfig.y1Col,
        segmentConfig.x2Col,
        segmentConfig.y2Col,
        segmentConfig.valueCol,
        if (xyBoundsFound) segmentConfig.projectionConfig.xyBounds else None,
        tilingConfig.levels,
        segmentConfig.tileSize,
        tms = tilingConfig.tms)(_)
      case _ => logger.error("Unknown projection ${topicsConfig.projection}"); sys.exit(-1)
    }

    val seqCols = segmentConfig.valueCol match {
      case None => Seq(segmentConfig.x1Col, segmentConfig.y1Col, segmentConfig.x2Col, segmentConfig.y2Col)
      case _ => Seq(
        segmentConfig.x1Col,
        segmentConfig.y1Col,
        segmentConfig.x2Col,
        segmentConfig.y2Col,
        segmentConfig.valueCol.getOrElse(throw new Exception("Value column is not set")))
    }
    val selectCols = seqCols.map(new Column(_))

    // Pipe the dataframe
    Pipe(dataframeFromSparkCsv(config, tilingConfig.source, schema, sparkSession))
      .to(_.select(selectCols: _*))
      .to(_.cache())
      .to(segmentOperation)
      .to(writeMetadata(config))
      .to(serializeBinArray)
      .to(outputOperation)
      .run()
  }

  private def writeMetadata[BC, V](baseConfig: Config)
                                  (tiles: RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]]):
  RDD[SeriesData[(Int, Int, Int), BC, V, (Double, Double)]] = {
    import net.liftweb.json.JsonAST._ // scalastyle:ignore
    import net.liftweb.json.JsonDSL._ // scalastyle:ignore

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
  }  // scalastyle:on cyclomatic.complexity
}
