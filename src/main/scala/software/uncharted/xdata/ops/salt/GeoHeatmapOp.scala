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
import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.numeric.{SumAggregator, MinMaxAggregator}
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.mapreduce.MapReduceTileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest

case class GeoHeatmapOpConf(levels: Int,
                            lonCol: Int,
                            latCol: Int,
                            timeCol: Int,
                            valueCol: Option[Int] = None,
                            timeRange: RangeDescription[Long],
                            xyBinCount: Int)

object GeoHeatmapOp {

  def apply(conf: GeoHeatmapOpConf)(dataFrame: DataFrame):
    RDD[SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]] = {
    geoHeatmapOp(conf)(dataFrame)
  }

  def geoHeatmapOp(conf: GeoHeatmapOpConf)(dataFrame: DataFrame):
    RDD[SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]] = {

    // Extracts lat / lon coordinates from row
    val coordExtractor = (r: Row) => {
      if (!r.isNullAt(conf.lonCol) && !r.isNullAt(conf.latCol) && !r.isNullAt(conf.timeCol)) {
        Some(r.getAs[Double](conf.lonCol), r.getAs[Double](conf.latCol), r.getAs[Long](conf.timeCol))
      } else {
        None
      }
    }

    // Extracts value data from row
    val valueExtractor: Option[(Row) => Option[Double]] = conf.valueCol match {
      case Some(idx: Int) => Some((r: Row) => {
        if (!r.isNullAt(idx)) Some(r.getAs[Double](idx)) else None
      })
      case _ => Some((r: Row) => { Some(1.0) })
    }

    // create a default projection from data-space into mercator tile space
    val projection = new MercatorTimeProjection(conf.timeRange)

    // create the series to tie everything together
    val series = new Series(
      (conf.xyBinCount-1, conf.xyBinCount-1, conf.timeRange.count-1),
      coordExtractor,
      projection,
      valueExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )

    val generator = new MapReduceTileGenerator(dataFrame.sqlContext.sparkContext)

    val request = new TileLevelRequest(List.range(0, conf.levels), (tc: (Int, Int, Int)) => tc._1)

    generator.generate(dataFrame.rdd, series, request).map(t => series(t))
  }
}
