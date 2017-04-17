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

package software.uncharted.salt.xdata.projection

import software.uncharted.salt.core.projection.numeric.MercatorProjection
import software.uncharted.salt.xdata.util.RangeDescription

object MercatorTimeProjection  {
  val minLon = -180.0
  val maxLon = 180.0
  val minLat = -85.05112878
  val maxLat = 85.05112878
}

class MercatorTimeProjection(zoomLevels: Seq[Int],
                             min: (Double, Double, Long) = (MercatorTimeProjection.minLon, MercatorTimeProjection.minLat, 0),
                             max: (Double, Double, Long) = (MercatorTimeProjection.maxLon, MercatorTimeProjection.maxLat, Long.MaxValue),
                             rangeBuckets: Long = Long.MaxValue,
                             tms: Boolean = true)
  extends XYTimeProjection(min, max, rangeBuckets, new MercatorProjection(zoomLevels, (min._1, min._2), (max._1, max._2), tms)) {

  def this(zoomLevels: Seq[Int], timeRange: RangeDescription[Long]) = {
    this(zoomLevels, (MercatorTimeProjection.minLon, MercatorTimeProjection.minLat, timeRange.min),
      (MercatorTimeProjection.maxLon, MercatorTimeProjection.maxLat, timeRange.max),
    timeRange.count)
  }
}
