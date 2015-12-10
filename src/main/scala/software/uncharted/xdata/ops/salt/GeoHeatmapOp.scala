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
import software.uncharted.salt.core.projection.numeric.MercatorProjection

object GeoHeatmapOp {

  def geoHeatmapOp(levels: Int, lonCol: Int, latCol: Int, valueCol: Option[Int])(dataFrame: DataFrame):
    RDD[SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]] = {

    // Extracts lat / lon coordinates from row
    val coordExtractor = (r: Row) => {
      if (!r.isNullAt(lonCol) && !r.isNullAt(latCol)) {
        Some(r.getAs[Double](lonCol), r.getAs[Double](latCol))
      } else {
        None
      }
    }

    // Extracts value data from row
    val valueExtractor: Option[(Row) => Option[Double]] = valueCol match {
      case Some(idx: Int) => Some((r: Row) => {
        if (!r.isNullAt(idx)) Some(r.getAs[Double](idx)) else None
      })
      case _ => Some((r: Row) => { Some(1.0) })
    }

    // create a default projection from data-space into mercator tile space
    val projection = new MercatorProjection()

    // create the series to tie everything together
    val series = new Series(
      (255, 255),
      coordExtractor,
      projection,
      valueExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )

    val generator = new MapReduceTileGenerator(dataFrame.sqlContext.sparkContext)

    val request = new TileLevelRequest(List.range(0, levels), (tc: (Int, Int, Int)) => tc._1)

    generator.generate(dataFrame.rdd, series, request).map(t => series(t))
  }
}
