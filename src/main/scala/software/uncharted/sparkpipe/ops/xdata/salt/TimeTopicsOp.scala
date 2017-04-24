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
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.salt.xdata.projection.XYTimeProjection
import software.uncharted.sparkpipe.ops.xdata.text.util.RangeDescription

object TimeTopicsOp extends XYTimeOp {

  def apply(// scalastyle:ignore
            baseProjection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
            xCol: String,
            yCol: String,
            rangeCol: String,
            textCol: String,
            timeRange: RangeDescription[Long],
            topicLimit: Int,
            zoomLevels: Seq[Int],
            tileSize: Int = 1)
           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), List[(String, Int)], Nothing]] = {

    // create a default projection from data-space into tile space
    val projection = new XYTimeProjection(timeRange.min, timeRange.max, timeRange.count, baseProjection)

    val aggregator = new TopElementsAggregator[String](topicLimit)

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    super.apply(projection,
                tileSize,
                xCol,
                yCol,
                rangeCol,
                textCol,
                aggregator,
                None
                )(request)(input)
  }
}
