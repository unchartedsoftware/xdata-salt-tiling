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
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.{TileLevelRequest, TileRequest}

object MercatorTimeTopics extends MercatorTimeOp {

  def apply(// scalastyle:ignore
            latCol: String,
            lonCol: String,
            rangeCol: String,
            textCol: String,
            latLonBounds: Option[(Double, Double, Double, Double)],
            timeRange: RangeDescription[Long],
            topicLimit: Int,
            zoomLevels: Seq[Int],
            tms: Boolean = true)
           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]] = {

    // Extracts value data from row
    val valueExtractor: Option[(Row) => Option[Seq[String]]] = Some((r: Row) => {
      val rowIndex = r.schema.fieldIndex(textCol)
      if (!r.isNullAt(rowIndex) && r.getSeq(rowIndex).nonEmpty) Some(r.getSeq(rowIndex)) else None
    })

    val aggregator = new TopElementsAggregator[String](topicLimit)

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    super.apply(latCol, lonCol, rangeCol, latLonBounds, timeRange, valueExtractor, aggregator, None, 1, tms)(request)(input)
  }
}

