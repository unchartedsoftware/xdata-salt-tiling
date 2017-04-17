/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.xdata.projection.CartesianTimeProjection
import software.uncharted.salt.xdata.util.RangeDescription

trait CartesianTimeOp extends XYTimeOp {

  def apply[T, U, V, W, X](// scalastyle:ignore
                           xCol: String,
                           yCol: String,
                           rangeCol: String,
                           bounds: Option[(Double, Double, Double, Double)],
                           timeRange: RangeDescription[Long],
                           valueExtractor: (Row) => Option[T],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           zoomLevels: Seq[Int],
                           tileSize: Int)
                           (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), V, X]] = {

    // create a default projection from data-space into mercator tile space
    val projection = bounds.map { b =>
      new CartesianTimeProjection(zoomLevels, (b._1, b._2, timeRange.min), (b._3, b._4, timeRange.max), timeRange.count)
    }.getOrElse(new CartesianTimeProjection(zoomLevels, (0.0, 0.0), (1.0, 1.0), timeRange))

    super.apply(xCol, yCol, rangeCol, timeRange, valueExtractor, binAggregator, tileAggregator,
      zoomLevels, tileSize, projection)(request)(input)
  }
}
