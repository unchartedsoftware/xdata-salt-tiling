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

import org.scalatest.FunSuite


class TwoStageLineProjectionTestSuite extends FunSuite {
  test("Leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((-40.0, -40.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    def checkSign(x: Int, sign: Int): Unit =
      if (0 != x) assert(sign === x.signum)
    val projection = new LeaderLineProjectionStageOne(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)
    for (xm <- -1 to 1 by 2; ym <- -1 to 1 by 2) {
      for (x0 <- 0 to 20; y0 <- 0 to 20; x1 <- x0 until 40; y1 <- y0 until 40) {
        val tiles =
          try {
            projection.project(Some((xm * x0.toDouble, ym * y0.toDouble, xm * x1.toDouble, ym * y1.toDouble)), maxBin).get.map(_._1).toSet
          } catch {
            case e: Exception =>
              throw new Exception("Error getting tiles for line [%d, %d => %d, %d]".format(x0, y0, x1, y1), e)
          }
        val bruteForceTiles = bruteForce.getTiles(xm * x0, ym * y0, xm * x1, ym * y1).toSet
        assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(xm * x0, ym * y0, xm * x1, ym * y1))
      }
    }
  }

  // A single-instance check, to make it easier to find the problem when the above exhaustive check fails
  ignore("Single instance leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((-40.0, -40.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    val projection = new LeaderLineProjectionStageOne(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)

    val x0 = -1
    val y0 = -2
    val x1 = -13
    val y1 = -16

    println("Checking [%d, %d -> %d, %d]".format(x0, y0, x1, y1))
    val bruteForceTiles = bruteForce.getTiles(x0, y0, x1, y1).toSet
    val tiles = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.map(_._1).toSet
    assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }
}

