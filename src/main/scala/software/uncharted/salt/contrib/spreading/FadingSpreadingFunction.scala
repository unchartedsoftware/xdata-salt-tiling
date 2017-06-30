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


package software.uncharted.salt.contrib.spreading

import software.uncharted.salt.core.spreading.SpreadingFunction
import software.uncharted.salt.contrib.projection.CartesianBinning

/**
  * Fade the ends of a line (as produced by the SimpleLeaderLineProjection) so they are bright near the endpoints,
  * and dull near the middle.
  */
class FadingSpreadingFunction(leaderLineLength: Int, maxBin: (Int, Int), _tms: Boolean)
  extends SpreadingFunction[(Int, Int, Int), (Int, Int), Double]
    with CartesianBinning {
  override protected def tms: Boolean = _tms

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value             the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  override def spread(coordsTraversable: Traversable[((Int, Int, Int), (Int, Int))],
                      value: Option[Double]): Traversable[((Int, Int, Int), (Int, Int), Option[Double])] = {
    val coords = coordsTraversable.toSeq
    val n = coords.length
    val halfWay = n / 2

    val all =
      if (n < 2 * leaderLineLength) {
        true
      } else {
        // Figure out if the midpoints are more than one bin apart
        val midBinLeft = coords(halfWay - 1)
        val midBinRight = coords(halfWay)
        val midUBinLeft = tileBinIndexToUniversalBinIndex(midBinLeft._1, midBinLeft._2, maxBin)
        val midUBinRight = tileBinIndexToUniversalBinIndex(midBinRight._1, midBinRight._2, maxBin)
        (midUBinLeft._1 - midUBinRight._1).abs < 2 && (midUBinLeft._2 - midUBinRight._2).abs < 2
      }

    if (all) {
      // No gaps, so just use all points without scaling
      coords.map { case (tile, bin) => (tile, bin, value) }
    } else {
      // Gap in the middle; scale points according to their distance from the end
      var i = 0
      coords.map { case (tile, bin) =>
        val scale =
          if (i < halfWay) {
            (halfWay - i).toDouble / halfWay
          } else {
            (i - halfWay + 1).toDouble / halfWay
          }
        i += 1

        (tile, bin, value.map(v => v * scale))
      }
    }
  }
}
