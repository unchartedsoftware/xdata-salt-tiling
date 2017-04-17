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
import org.apache.spark.sql.DataFrame
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

    super.apply(projection, tileSize, xCol, yCol, vCol, binAggregator, tileAggregator)(request)(input)
  }
}
object CartesianOp extends CartesianOp
