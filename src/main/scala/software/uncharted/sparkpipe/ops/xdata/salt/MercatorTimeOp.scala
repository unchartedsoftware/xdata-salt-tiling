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
package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.xdata.projection.MercatorTimeProjection
import software.uncharted.salt.xdata.util.RangeDescription

trait MercatorTimeOp extends XYTimeOp {

  def apply[T, U, V, W, X](// scalastyle:ignore
                           latCol: String,
                           lonCol: String,
                           rangeCol: String,
                           lonLatBounds: Option[(Double, Double, Double, Double)],
                           timeRange: RangeDescription[Long],
                           valueExtractor: (Row) => Option[T],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           zoomLevels: Seq[Int],
                           tileSize: Int,
                           tms: Boolean)
                          (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), V, X]] = {
    // create a default projection from data-space into mercator tile space
    val projection = lonLatBounds.map { b =>
      new MercatorTimeProjection(zoomLevels, (b._1, b._2, timeRange.min), (b._3, b._4, timeRange.max), timeRange.count, tms)
    }.getOrElse(new MercatorTimeProjection(zoomLevels, timeRange))

    super.apply(lonCol, latCol, rangeCol, timeRange, valueExtractor, binAggregator, tileAggregator,
      zoomLevels, tileSize, projection)(request)(input)
  }
}