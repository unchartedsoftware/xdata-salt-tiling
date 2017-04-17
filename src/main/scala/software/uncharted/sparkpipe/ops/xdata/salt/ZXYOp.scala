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
import org.apache.spark.sql.{Column, DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns

object ZXYOp extends ZXYOp {
  final val TILE_SIZE_DEFAULT = 256
}
/**
  * A superclass for operations which generate
  * zxy tile layers from a DataFrame, using Salt
  */
trait ZXYOp {
  // scalastyle:off parameter.number
  private val DOUBLE_TYPE = "double"
  /**
    * Main tiling operation.
    *
    * @param projection     a NumericProjection, which maps double x/y data-space coordinates into z/x/y tile coordinates
    * @param tileSize       the size of one side of a tile, in bins (i.e. 256 for a 256x256 tile)
    * @param xCol           the name of the x column
    * @param yCol           the name of the y column
    * @param vCol           the name of the value column (the value for aggregation)
    * @param binAggregator  an Aggregator which aggregates values from the ValueExtractor
    * @param tileAggregator an optional Aggregator which aggregates bin values
    */
  def apply[T, U, V, W, X](projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
                           tileSize: Int,
                           xCol: String,
                           yCol: String,
                           vCol: String,
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val selectCols = Seq(xCol, yCol, vCol).map(new Column(_))

    // Use the pipeline to convert x/y cols to doubles, and select them along with v col first
    val frame = Pipe(input)
      .to(castColumns(Map(xCol -> DOUBLE_TYPE, yCol -> DOUBLE_TYPE)))
      .to(_.select(selectCols: _*))
      .run()

    val cExtractor = (r: Row) => {
      val xIndex = r.schema.fieldIndex(xCol)
      val yIndex = r.schema.fieldIndex(yCol)
      if (!r.isNullAt(xIndex) && !r.isNullAt(yIndex)) {
        Some((r.getDouble(xIndex), r.getDouble(yIndex)))
      } else {
        None
      }
    }

    val vExtractor = (r: Row) => {
      val rowIndex = r.schema.fieldIndex(vCol)
      if (!r.isNullAt(rowIndex)) {
        Some(r.getAs[T](rowIndex))
      } else {
        None
      }
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      binAggregator,
      tileAggregator
    )

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}
