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
import software.uncharted.salt.core.projection.numeric.MercatorProjection

object MercatorHeatmapOp {

  def apply(// scalastyle:ignore
            latCol: String,
            lonCol: String,
            valueCol: String,
            zoomLevels: Seq[Int],
            latLonBounds: Option[(Double, Double, Double, Double)] = None,
            tileSize: Int = ZXYOp.TILE_SIZE_DEFAULT
           )(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {


    val projection = {
      if (latLonBounds.isEmpty) {
        new MercatorProjection(zoomLevels)
      } else {
        val geo_bounds = latLonBounds.get
        new MercatorProjection(zoomLevels, (geo_bounds._1,  geo_bounds._2), (geo_bounds._3,  geo_bounds._4))
      }
    }

    val vExtractor = (r: Row) => {
      if (!r.isNullAt(2)) {
        Some(r.getAs[Double](2))
      } else {
        None
      }
    }
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    ZXYOp(
      projection,
      tileSize,
      latCol,
      lonCol,
      valueCol,
      vExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )(request)(input)

  }
}
