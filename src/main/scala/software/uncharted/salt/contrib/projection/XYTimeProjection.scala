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

package software.uncharted.salt.contrib.projection

import software.uncharted.salt.core.projection.Projection
import software.uncharted.salt.core.projection.numeric.NumericProjection

/**
  * Projects a data space x,y,timestamp coordinate into the corresponding (tile, bin) coordinate.  Values that lie outside
  * the defined x,y or timestamp ranges will be ignored.
  *
  * @param min The minimum time value to project.
  * @param max The maximum time value to project.
  * @param rangeBuckets The number of time buckets between (min, max) to project into.
  * @param baseProjection The (x,y) coordinate projection to apply.
  */
class XYTimeProjection(val min: Long,
                       val max: Long,
                       val rangeBuckets: Int,
                       val baseProjection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)])
  extends Projection[(Double, Double, Long), (Int, Int, Int), (Int, Int, Int)] {

  override def project (dCoords: Option[(Double, Double, Long)], maxBin: (Int, Int, Int)):
  Option[Seq[((Int, Int, Int), (Int, Int, Int))]] = {
    dCoords.flatMap { coords =>
      if (coords._3 >= min && coords._3 <= max) {
        baseProjection.project(Some((coords._1, coords._2)), (maxBin._1, maxBin._2))
          .flatMap { projectedCoords =>
            Some(
              projectedCoords.map { projCoord =>
                val binSize = (max - min) / rangeBuckets
                val timeBin = (coords._3 - min) / binSize
                if (timeBin < rangeBuckets) {
                  Some(projCoord._1, (projCoord._2._1, projCoord._2._2, timeBin.asInstanceOf[Int]))
                } else {
                  None
                }
              }.toSeq.flatten
            )
          }
      } else {
        None
      }
    }
  }

  override def binTo1D(bin: (Int, Int, Int), maxBin: (Int, Int, Int)): Int = {
    val result = (maxBin._1 + 1) * (maxBin._2 + 1) * bin._3 + (maxBin._1 + 1) * bin._2 + bin._1
    result
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int, Int)): (Int, Int, Int) = {
    // i = xSize * ySize * z + xSize * y + x
    val xSize = maxBin._1 + 1
    val ySize = maxBin._2 + 1
    val xy = index % (xSize * ySize)
    val time = (index - xy) / (xSize * ySize)
    val x = xy % xSize
    val y = (xy - x) / xSize
    (x, y, time)
  }
}
