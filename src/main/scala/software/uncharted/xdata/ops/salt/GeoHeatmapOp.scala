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
package software.uncharted.xdata.ops.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{TimestampType, DoubleType}
import org.apache.spark.sql.{Column, Row, DataFrame}
import software.uncharted.salt.core.analytic.numeric.{SumAggregator, MinMaxAggregator}
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.mapreduce.MapReduceTileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.{castColumns, renameColumns}

case class GeoHeatmapOpConf(lonCol: String,
                            latCol: String,
                            timeCol: String,
                            valueCol: Option[String],
                            latLonBounds: Option[(Double, Double, Double, Double)],
                            timeRange: RangeDescription[Long],
                            levels: Int,
                            tileSize: Int = GeoHeatmapOp.defaultTileSize)

object GeoHeatmapOp {

  val maxLon = 180
  val maxLat = 85.05112878
  val defaultTileSize = 256

  def apply(conf: GeoHeatmapOpConf)(input: DataFrame):
    RDD[SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]] = {
    // Use the pipeline to cast columns to expected values and select them into a new dataframe
    val selectCols = (Seq(conf.latCol, conf.lonCol, conf.timeCol) ++ conf.valueCol)
      .map(new Column(_))

    val castCols = (Seq(conf.latCol -> DoubleType.typeName, conf.lonCol -> DoubleType.typeName, conf.timeCol -> TimestampType.typeName) ++
      conf.valueCol.map(v => v -> DoubleType.typeName)).toMap

    val frame = Pipe(input)
      .to(castColumns(castCols))
      .to(_.select(selectCols:_*))
      .run()

    // Extracts lat, lon, time coordinates from row - can assume (0,1,2) indices given select above
    val coordExtractor = (r: Row) => {
      if (!r.isNullAt(0) && !r.isNullAt(1) && !r.isNullAt(2)) {
        Some(r.getDouble(1), r.getDouble(0), r.getTimestamp(2).getTime)
      } else {
        None
      }
    }

    // Extracts value data from row
    val valueExtractor: Option[(Row) => Option[Double]] = conf.valueCol match {
      case Some(colName: String) => Some((r: Row) => {
        if (!r.isNullAt(3)) Some(r.getDouble(3)) else None
      })
      case _ => Some((r: Row) => { Some(1.0) })
    }

    // create a default projection from data-space into mercator tile space
    val projection = conf.latLonBounds.map { b =>
      new MercatorTimeProjection((b._2, b._1, conf.timeRange.min), (b._4, b._3, conf.timeRange.max), conf.timeRange.count)
    }.getOrElse(new MercatorTimeProjection(conf.timeRange))

    // create the series to tie everything together
    val series = new Series(
      (conf.tileSize-1, conf.tileSize-1, conf.timeRange.count-1),
      coordExtractor,
      projection,
      valueExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )

    val generator = new MapReduceTileGenerator(frame.sqlContext.sparkContext)
    val request = new TileLevelRequest(List.range(0, conf.levels), (tc: (Int, Int, Int)) => tc._1)
    generator.generate(frame.rdd, series, request).map(t => series(t))
  }
}
