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
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.MercatorProjection

trait MercatorOp extends ZXYOp {

  def apply[T, U, V, W, X](// scalastyle:ignore
                           latCol: String,
                           lonCol: String,
                           valueCol: String,
                           lonLatBounds: Option[(Double, Double, Double, Double)],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           zoomLevels: Seq[Int],
                           tileSize: Int,
                           tms: Boolean)
                          (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    // create a default projection from data-space into mercator tile space
    val projection = lonLatBounds.map { b =>
      new MercatorProjection(zoomLevels, (b._1, b._2),(b._3, b._4), tms)
    }.getOrElse(new MercatorProjection(zoomLevels))

    super.apply(
                projection,
                tileSize,
                lonCol,
                latCol,
                valueCol,
                binAggregator,
                tileAggregator)(request)(input)
  }
}
