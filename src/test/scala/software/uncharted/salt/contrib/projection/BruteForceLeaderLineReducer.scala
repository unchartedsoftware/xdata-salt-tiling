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

import software.uncharted.salt.contrib.projection.geometry.{Line, LineToPoints}


/*
 * Use Line2Points to figure out leader line expansions of lines by brute force, to use when testing more
 * refined methods.
 */
class BruteForceLeaderLineReducer (maxBin: (Int, Int),
                                   bounds: ((Double, Double), (Double, Double)),
                                   level: Int,
                                   leaderLength: Int,
                                   tms: Boolean)
  extends CartesianTileProjection2D[(Int, Int, Int), ((Int, Int), (Int, Int))](bounds._1, bounds._2, tms)
{
  def getUniversalBins (x0: Double, y0: Double, x1: Double, y1: Double) = {
    def project(x: Double, y: Double) = {
      val scale = 1 << level
      val usx = (x - bounds._1._1) / (bounds._2._1 - bounds._1._1)
      val usy = (y - bounds._1._2) / (bounds._2._2 - bounds._1._2)
      val maxy = scale * (maxBin._2 + 1)
      val sx = usx * scale * (maxBin._1 + 1)
      val sy = maxy - usy * scale * (maxBin._2 + 1)
      (sx.floor.toInt, sy.floor.toInt)
    }

    val s = project(x0, y0)
    val e = project(x1, y1)

    val line = new LineToPoints(s, e)

    import Line.{distance, intPointToDoublePoint}

    line.rest().filter(p => distance(p, s) <= leaderLength || distance(p, e) <= leaderLength)
  }

  def getBins (x0: Double, y0: Double, x1: Double, y1: Double) = {
    getUniversalBins(x0, y0, x1, y1).map{uBin =>
      universalBinIndexToTileIndex(level, uBin, maxBin)
    }
  }

  def getTiles (x0: Double, y0: Double, x1: Double, y1: Double) = {
    val points = getUniversalBins(x0, y0, x1, y1)

    val closeTiles = points.map(p => universalBinIndexToTileIndex(level, p, maxBin)._1)

    closeTiles.distinct
  }

  // Just stub implementations of projection functions - we aren't testing those.
  override def project(dc: Option[(Int, Int, Int)], maxBin: ((Int, Int), (Int, Int))): Option[Seq[((Int, Int, Int), ((Int, Int), (Int, Int)))]] = None
  override def binTo1D(bin: ((Int, Int), (Int, Int)), maxBin: ((Int, Int), (Int, Int))): Int = 0
  override def binFrom1D(index: Int, maxBin: ((Int, Int), (Int, Int))): ((Int, Int), (Int, Int)) = ((0, 0), (0, 0))
}
