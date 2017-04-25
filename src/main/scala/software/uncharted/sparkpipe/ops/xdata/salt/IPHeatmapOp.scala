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
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.xdata.projection.IPProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe

/**
  * An operation which generates tile layers of point locations, located by IP address, from a DataFrame, using Salt
  */
object IPHeatmapOp {
  val DefaultTileSize = 256
  val StringType = "string"

  def apply(ipCol: String,
            vCol: String,
            levels: Seq[Int],
            tileSize: Int = DefaultTileSize)
           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {

    val projection = new IPProjection(levels)

    val frame = Pipe(input).to(dataframe.castColumns(Map(ipCol -> StringType))).run()

    val cExtractor = (r: Row) => {
      val ipIndex = r.schema.fieldIndex(ipCol)
      if (!r.isNullAt(ipIndex)) {
        Some(r.getString(ipIndex))
      } else {
        None
      }
    }

    val vExtractor = (r: Row) => {
      val rowIndex = r.schema.fieldIndex(vCol)
      if (!r.isNullAt(rowIndex)) {
        Some(r.getDouble(rowIndex))
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
      SumAggregator,
      Some(MinMaxAggregator)
    )

    val request = new TileLevelRequest(levels, (tc: (Int, Int, Int)) => tc._1)

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}
