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

object MercatorTimeHeatmap extends MercatorTimeOp {

  def apply(// scalastyle:ignore
            latCol: String,
            lonCol: String,
            rangeCol: String,
            valueCol: Option[String],
            latLonBounds: Option[(Double, Double, Double, Double)],
            timeRange: RangeDescription[Long],
            zoomLevels: Seq[Int],
            tileSize: Int = defaultTileSize,
            tms: Boolean = true)
           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), Double, (Double, Double)]] = {

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
    super.apply(latCol, lonCol, rangeCol, latLonBounds, timeRange, valueExtractor, SumAggregator,
      Some(MinMaxAggregator), zoomLevels, tileSize, tms)(request)(input)
  }
}
