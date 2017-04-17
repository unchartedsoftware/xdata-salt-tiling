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

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    ZXYOp(
      projection,
      tileSize,
      latCol,
      lonCol,
      valueCol,
      SumAggregator,
      Some(MinMaxAggregator)
    )(request)(input)

  }
}
