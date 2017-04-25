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
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.xdata.projection.MercatorLineProjection

import scala.util.Try

/**
  * Segment operation using the mercator projection
  */
object MercatorSegmentOp {
  // scalastyle:off method.length
  val DefaultTileSize = 256
  val DefaultMaxSegLen = 1024

  /**
    * Segment operation using the mercator projection
    *
    * @param minSegLen  The minimum length of line (in bins) to project
    * @param maxSegLen  The maximum length of line (in bins) to project
    * @param x1Col      The start x coordinate
    * @param y1Col      The start y coordinate
    * @param x2Col      The end x coordinate
    * @param y2Col      The end y coordinate
    * @param valueCol   The name of the column which holds the value for a given row
    * @param xyBounds   The min/max x and y bounds (minX, minY, maxX, maxY)
    * @param zoomLevels The zoom levels onto which to project
    * @param tileSize   The size of a tile in bins
    * @param tms        If true, the Y axis for tile coordinates only is flipped
    * @param input      The input data
    * @return An RDD of tiles
    */
  // scalastyle:off parameter.number
  def apply(minSegLen: Option[Int],
            maxSegLen: Option[Int] = Some(DefaultMaxSegLen),
            x1Col: String,
            y1Col: String,
            x2Col: String,
            y2Col: String,
            valueCol: Option[String],
            xyBounds: Option[(Double, Double, Double, Double)],
            zoomLevels: Seq[Int],
            tileSize: Int = DefaultTileSize)
           (input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {

    val valueExtractor: (Row) => Option[Double] = valueCol match {
      case Some(colName: String) => (r: Row) => {
        val rowIndex = r.schema.fieldIndex(colName)
        if (!r.isNullAt(rowIndex)) Some(r.getDouble(rowIndex)) else None
      }
      case _ => (r: Row) => {
        Some(1.0)
      }
    }

    // A coordinate extractor to pull out our endpoint coordinates
    val x1Pos = input.schema.zipWithIndex.find(_._1.name == x1Col).map(_._2).getOrElse(-1)
    val y1Pos = input.schema.zipWithIndex.find(_._1.name == y1Col).map(_._2).getOrElse(-1)
    val x2Pos = input.schema.zipWithIndex.find(_._1.name == x2Col).map(_._2).getOrElse(-1)
    val y2Pos = input.schema.zipWithIndex.find(_._1.name == y2Col).map(_._2).getOrElse(-1)
    val coordinateExtractor = (row: Row) =>
      Try((row.getDouble(x1Pos), row.getDouble(y1Pos), row.getDouble(x2Pos), row.getDouble(y2Pos))).toOption

    // Figure out our projection
    var minBounds = (-180.0, -85.05112878)
    var maxBounds = (180.0, 85.05112878)
    if (xyBounds.isDefined) {
      val geo_bounds = xyBounds.get
      minBounds = (geo_bounds._1, geo_bounds._2)
      maxBounds = (geo_bounds._3, geo_bounds._4)
    }
    val projection = new MercatorLineProjection(zoomLevels, minBounds, maxBounds, minSegLen, maxSegLen)

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    val series = new Series(
      // Maximum bin indices
      (tileSize - 1, tileSize - 1),
      coordinateExtractor,
      projection,
      valueExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )

    BasicSaltOperations.genericTiling(series)(request)(input.rdd)
  }
}
