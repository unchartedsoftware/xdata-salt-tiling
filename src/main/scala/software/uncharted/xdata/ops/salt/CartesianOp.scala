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
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.projection.numeric.CartesianProjection

/**
  * An operation for generating a cartesian-projected
  * tile layer from a DataFrame, using Salt
  */



class CartesianOp extends ZXYOp {

  // scalastyle:off parameter.number

  /**
    * Main tiling operation.
    *
    * @param tileSize       the size of one side of a tile, in bins
    * @param xCol           the name of the x column
    * @param yCol           the name of the y column
    * @param vCol           the name of the value column (the value for aggregation)
    * @param xyBounds       the y/X bounds of the "world" for this tileset, specified as (minX, minY, maxX, maxY)
    * @param zoomLevels     the zoom bounds of the "world" for this tileset, specified as a Seq of Integer zoom levels
    * @param binAggregator  an Aggregator which aggregates values from the ValueExtractor
    * @param tileAggregator an optional Aggregator which aggregates bin values
    */
  def apply[T, U, V, W, X](tileSize: Int = ZXYOp.TILE_SIZE_DEFAULT,
                           xCol: String,
                           yCol: String,
                           vCol: String,
                           xyBounds: (Double, Double, Double, Double),
                           zoomLevels: Seq[Int],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val projection = new CartesianProjection(zoomLevels, (xyBounds._1, xyBounds._2), (xyBounds._3, xyBounds._4))
    val vExtractor = (r: Row) => {
      if (!r.isNullAt(2)) {
        Some(r.getAs[T](2))
      } else {
        None
      }
    }

    super.apply(projection, tileSize, xCol, yCol, vCol, vExtractor, binAggregator, tileAggregator)(request)(input)
  }
}
object CartesianOp extends CartesianOp
