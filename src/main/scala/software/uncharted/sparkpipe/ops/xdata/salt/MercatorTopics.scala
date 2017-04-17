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
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest

object MercatorTopics extends MercatorOp {

  def apply(// scalastyle:ignore
            latCol: String,
            lonCol: String,
            textCol: String,
            latLonBounds: Option[(Double, Double, Double, Double)],
            topicLimit: Int,
            zoomLevels: Seq[Int],
            tileSize: Int = 1,
            tms: Boolean = true)
            (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Int)], Nothing]] = {

    val aggregator = new TopElementsAggregator[String](topicLimit)

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    super.apply(
      latCol,
      lonCol,
      textCol,
      latLonBounds,
      aggregator,
      None,
      zoomLevels,
      tileSize,
      tms)(request)(input)
  }
}

