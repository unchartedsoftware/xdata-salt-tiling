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
import org.apache.spark.sql.{DataFrame, functions}
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.projection.numeric.NumericProjection

object HeatmapOp extends ZXYOp {

  val DefaultTileSize = 256

  def apply(// scalastyle:ignore
            xCol: String,
            yCol: String,
            valueCol: Option[String],
            projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
            zoomLevels: Seq[Int],
            tileSize: Int = DefaultTileSize
           )(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    // if there is no value column specified update the input data frame with a new
    // column of ones
    val updated = valueCol.map(v => (v, input))
      .getOrElse(("__count__", input.withColumn("__count__", functions.lit(1.0))))

    super.apply(
      projection,
      tileSize,
      xCol,
      yCol,
      updated._1,
      SumAggregator,
      Some(MinMaxAggregator)
    )(request)(updated._2)
  }
}
