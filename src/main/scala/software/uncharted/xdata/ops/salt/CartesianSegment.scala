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
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest

/**
  * Create a
  * - valueExtractor: Row => Option[T],
  * - binAggregator: Aggregator[T, U, V],
  * - tileAggregator: Option[Aggregator[V, W, X]]
  * Pass these, along with own arguments to super.
  *
  **/
object CartesianSegment extends CartesianSegmentOp {

  /**
    * TODO
    **/
  def apply( // scalastyle:off parameter.number
    arcType: ArcTypes.Value,
    minSegLen: Option[Int],
    maxSegLen: Option[Int],
    x1Col: String,
    y1Col: String,
    x2Col: String,
    y2Col: String,
    valueCol: Option[String],
    xyBounds: (Double, Double, Double, Double),
    zBounds: (Int, Int),
    zoomLevels: Seq[Int],
    tileSize: Int)
    (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), Double, (Double, Double)]] = {

    // TODO This is a copy from CartesianTimeHeatmap. Is it what we need?
    // Extracts value data from row
    val valueExtractor: (Row) => Option[Double] = valueCol match {
      case Some(colName: String) => (r: Row) => {
        val rowIndex = r.schema.fieldIndex(colName)
        if (!r.isNullAt(rowIndex)) Some(r.getDouble(rowIndex)) else None
      }
      case _ => (r: Row) => {
        Some(1.0)
      }
    }

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    super.apply(
      arcType,
      minSegLen,
      maxSegLen,
      x1Col,
      y1Col,
      x2Col,
      y2Col,
      xyBounds,
      zBounds,
      valueExtractor,
      SumAggregator, // TODO This is a copy from CartesianTimeHeatmap, as advised by Nathan. Does it work?
      Some(MinMaxAggregator), // TODO This is a copy from CartesianTimeHeatmap, as advised by Nathan. Does it work?
      tileSize
    )(request)(input)
  }
}
