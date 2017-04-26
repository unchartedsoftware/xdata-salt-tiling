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
import org.scalatest.Matchers._
import software.uncharted.salt.xdata.projection.geometry.Line
import software.uncharted.salt.xdata.spreading.FadingSpreadingFunction

class OneStageLineProjectionTestSuite extends FunSuite {
  //SimpleLeaderLineProjection tests
  test("Test lots of possible leader lines") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms = tms)
    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
          val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
          assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
        } catch {
          case e: Exception => throw new Exception("Error processing point [%d, %d x %d, %d".format(x0, y0, x1, y1), e)
        }
      }
    }
  }
  test("Test giving empty coords to project method of SimpleLeaderLineProjection") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = tms)
    val result = projection.project(None, maxBin)

    assertResult(None)(result)
  }
  test("Test when line2point is greater than minLengthOpt") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, Some(2), tms = tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms = tms)

    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
          val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
          assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
        } catch {
          case e: Exception => throw new Exception("Error processing point [%d, %d x %d, %d".format(x0, y0, x1, y1), e)
        }
      }
    }
  }
  test("Test when line2point is not greater than minLengthOpt") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false
    val minLength = 1000

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, Some(minLength), tms = tms)

    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
          assert(usingProjection == None)
        } catch {
          case e: Exception => throw new Exception(s"Error thrown: $e")
        }
      }
    }
  }
  test("Test binTo1D and binFrom1D functions in SimpleLeaderLineProjection") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false
    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = tms)

    val coord1D = projection.binTo1D((2, 2), maxBin)
    assertResult(10)(coord1D)

    val binCoord = projection.binFrom1D(9, maxBin)
    assertResult((1, 2))(binCoord)
  }

  // This test is here to activate in case anything fails in the previous test, so as to make debugging it simpler.
  ignore("Test a single case that failed in the exhaustive leader line test") {
    val (x0, y0, x1, y1) = (-10, -5, -9, -10)
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = true)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms = true)

    val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
    val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
    assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }

  //SimpleLineProjection tests
  test("Test lots of possible full lines") {
    // 2x2-bin tiles and a data range of 32 should give us a data range of 1 per bin
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)
    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val expectedLineLength = math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0))
          val bins = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
          if (expectedLineLength >= 3.0 && expectedLineLength <= 7.0) {
            val expectedPoints = (x1 - x0).abs max (y1 - y0).abs
            assert(bins.get.length === expectedPoints + 1)
          } else {
            assert(bins.isEmpty)
          }
        } catch {
          case e: Exception => throw new Exception("Error processing point [%d, %d x %d, %d".format(x0, y0, x1, y1), e)
        }
      }
    }
  }
  test("Test giving empty coords to project method of SimpleLineProjection") {
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)
    val result = projection.project(None, maxBin)

    assertResult(None)(result)
  }
  test("Test binTo1D and binFrom1D functions in SimpleLineProjection") {
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)

    val coord1D = projection.binTo1D((2, 2), (3, 3))
    assertResult(10)(coord1D)

    val binCoord = projection.binFrom1D(9, (3, 3))
    assertResult((1, 2))(binCoord)
  }

  // This test is here to activate in case anything fails in the previous test, so as to make debugging it simpler.
  ignore("Test a single case that failed in the exhaustive full line test") {
    val (x0, y0, x1, y1) = (-10, -10, -7, -7)
    // 2x2-bin tiles and a data range of 32 should give us a data range of 1 per bin
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)
    val expectedLineLength = math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0))
    val bins = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
    if (expectedLineLength >= 3.0 && expectedLineLength <= 7.0) {
      val expectedPoints = (x1 - x0).abs max (y1 - y0).abs
      assert(bins.get.length === expectedPoints + 1)
    } else {
      assert(bins.isEmpty)
    }
  }

  test("Test leader line fading spreader function") {
    val spreader = new FadingSpreadingFunction(4, (3, 3), false)
    assert(List(4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0) === spreader.spread(Seq(
      ((2, 0, 0), (0, 0)), ((2, 0, 0), (1, 0)), ((2, 0, 0), (2, 0)), ((2, 0, 0), (3, 0)),
      ((2, 1, 0), (0, 0)), ((2, 1, 0), (1, 0)), ((2, 1, 0), (2, 0)), ((2, 1, 0), (3, 0))
    ), Some(4.0)).map(_._3.get).toList)

    assert(List(4.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 4.0) === spreader.spread(Seq(
      ((2, 0, 0), (0, 0)), ((2, 0, 0), (1, 0)), ((2, 0, 0), (2, 0)), ((2, 0, 0), (3, 0)),
      ((2, 1, 0), (1, 0)), ((2, 1, 0), (2, 0)), ((2, 1, 0), (3, 0)), ((2, 2, 0), (0, 0))
    ), Some(4.0)).map(_._3.get).toList)
  }

  //SimpleLeaderArcProjection tests
  test("Test arc leader lines projection - no gap") {
    import Line.{distance, intPointToDoublePoint}

    val levels = 4
    val minBounds = (0.0, 0.0)
    val maxBounds = (64.0, 64.0)
    val leaderLength = 8

    val startCoord = (24.0, 24.0)
    val endCoord = (36.0, 33.0)
    val maxBin = (3, 3)

    val projection = new SimpleLeaderArcProjection(Seq(levels), minBounds, maxBounds, leaderLength) //tms is default to false
    val pointsOpt = projection.project(Some((startCoord._1, startCoord._2, endCoord._1, endCoord._2)), maxBin)

    // All are in universal bin coordinates, so Y axis is reversed from these calculations.
    // Also, clockwise becomes counterclockwise because of the y axis reversal

    // Points are (24, 24) and (36, 33), creating equilateral triangle arc
    // length of one side is sqrt(12^2 + 9^2) = 15
    val length = 15.0

    // converting startCoord and endCoord to be on flipped y axis
    val start = (24.0, maxBounds._2 - 24)
    val end = (36.0, maxBounds._2 - 33)

    //old calc of third point of triangle (called val center)
    //not using (start, end) which already corrected for y reversal.
    //x does not need to be shifted, since x flipping does not occur
    //val center = (midPtCoord._1 - 0.6 * medianLength, 64 - (midPtCoord._2 + 0.8 * medianLength))

    //TODO: calculation of third point of equilateral triangle (previously called val center)
    val midPtCoord = (30.0, 28.5) //30 is halfway point between 24 and 36 and same for 28.5 being halfway point for y coordinate
    val medianLength = 7.5 * math.sqrt(3) //tan(60 deg) is sqrt(3)

    val delta_y = 33- 24 //9, the difference between start and end y-coordinates
    val delta_x = 36 - 24 //12, the difference between start and end x-coordinates

    //these calculations are not correct yet
    val angle = 30.toRadians +  math.atan2(delta_y, delta_x) //opposite to angle is median, adjacent to angle is half of line drawn from startCoord to endCoord
    val x3 = midPtCoord._1 - medianLength * math.cos(angle)
    val y3 = midPtCoord._2 + medianLength * math.sin(angle)

    val thirdPoint = (x3, y3)

    println(distance(thirdPoint, startCoord)) //must equal 15
    println(distance(thirdPoint, endCoord)) //must equal 15

    assert(pointsOpt.isDefined)
    val points = pointsOpt.get
    points.foreach{point =>
      val (tile, bin) = point
      val uBin = (tile._2 * 4 + bin._1, tile._3 * 4 + bin._2) //returns same result as tileBinIndexToUniversalBinIndex
      println(point+"\t"+uBin)
      distance(thirdPoint, uBin) should be (15.0 +- math.sqrt(0.5))
      assert(distance(start, uBin) < 9.0 || distance(end, uBin) < 9.0)
      assert(start._1 <= uBin._1 && uBin._1 <= end._1)
      assert(start._2 >= uBin._2 && uBin._2 >= end._2) //comparison operators switched since flipped y aixs
    }
  }
  test("Test arc leader lines projection - with gap") {
    import Line.{distance, intPointToDoublePoint}

    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val pointsOpt = projection.project(Some((34.0, 17.0, 10.0, 7.0)), (3, 3))
    // All are in universal bin coordinates, so Y axis is reversed from these calculations.
    // Also, clockwise becomes counterclockwise because of the y axis reversal
    // Points are (34, 17), (10, 7), creating equilateral triangle arc
    // Length is 26
    // Center should be (22, 12) + (-5/13, 12/13) * 13 * sqrt(3)
    val start = (34, 64 - 17)
    val end = (10, 64 - 7)
    val center = (22.0 + 5.0 * math.sqrt(3.0), 64.0 - (12.0 - 12.0 * math.sqrt(3.0)))

    assert(pointsOpt.isDefined)
    val points = pointsOpt.get
    points.foreach{point =>
      val (tile, bin) = point
      val uBin = (tile._2 * 4 + bin._1, tile._3 * 4 + bin._2)
      distance(center, uBin) should be (26.0 +- math.sqrt(0.5))
      assert(distance(start, uBin) < 9.0 || distance(end, uBin) < 9.0)
      assert(end._1 <= uBin._1 && uBin._1 <= start._1)
      assert(uBin._2 >= start._2)
    }
  }
  test("Test giving empty coords to project method of SimpleLeaderArcProjection") {
    val maxBin = (3, 3)
    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val result = projection.project(None, maxBin)

    assertResult(None)(result)
  }
  test("Test when minLengthOpt is None") {
    import Line.{distance, intPointToDoublePoint}

    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8, minLengthOpt = None)
    val pointsOpt = projection.project(Some((34.0, 17.0, 10.0, 7.0)), (3, 3))

    val start = (34, 64 - 17)
    val end = (10, 64 - 7)
    val center = (22.0 + 5.0 * math.sqrt(3.0), 64.0 - (12.0 - 12.0 * math.sqrt(3.0)))

    assert(pointsOpt.isDefined)

    val points = pointsOpt.get
    points.foreach{point =>
      val (tile, bin) = point
      val uBin = (tile._2 * 4 + bin._1, tile._3 * 4 + bin._2)
      distance(center, uBin) should be (26.0 +- math.sqrt(0.5))
      assert(distance(start, uBin) < 9.0 || distance(end, uBin) < 9.0)
      assert(end._1 <= uBin._1 && uBin._1 <= start._1)
      assert(uBin._2 >= start._2)
    }
  }
  test("Test when minLength is greater than chordLength") {
    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8, minLengthOpt = Some(100))
    val pointsOpt = projection.project(Some((34.0, 17.0, 10.0, 7.0)), (3, 3))

    assert(pointsOpt.isEmpty)
  }
  test("Test binTo1D and binFrom1D functions in SimpleLeaderArcProjection") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false
    val projection = new SimpleLeaderArcProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = tms)

    val coord1D = projection.binTo1D((2, 2), (3, 3))
    assertResult(10)(coord1D)

    val binCoord = projection.binFrom1D(9, (3, 3))
    assertResult((1, 2))(binCoord)
  }

  //SimpleArcProjection tests
  test("Test giving empty coords to project method of SimpleArcProjection") {
    val maxBin = (3, 3)
    val projection = new SimpleArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val result = projection.project(None, maxBin)

    assertResult(None)(result)
  }
  test("Test binTo1D and binFrom1D functions in SimpleArcProjection") {
    val maxBin = (3, 3)
    val projection = new SimpleArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val result = projection.project(None, maxBin)

    val coord1D = projection.binTo1D((2, 2), (3, 3))
    assertResult(10)(coord1D)

    val binCoord = projection.binFrom1D(9, (3, 3))
    assertResult((1, 2))(binCoord)
  }
  test("Test SimpleArcProjection projection") {

    val projection = new SimpleArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val pointsOpt = projection.project(Some((24.0, 24.0, 36.0, 33.0)), (3, 3))
    assert(pointsOpt.isDefined)

    val points = pointsOpt.get
    val expected = Seq(
      ((4,6,10),(0,0)),
      ((4,6,9),(0,3)),
      ((4,6,9),(0,2)),
      ((4,6,9),(1,1)),
      ((4,6,9),(1,0)),
      ((4,6,8),(2,3)),
      ((4,6,8),(3,2)),
      ((4,7,8),(0,1)),
      ((4,7,8),(1,0)),
      ((4,7,8),(2,0)),
      ((4,7,7),(3,3)),
      ((4,8,7),(0,3)),
      ((4,8,7),(1,3)),
      ((4,8,7),(2,3)),
      ((4,8,7),(3,3)),
      ((4,9,7),(0,3))
    )
    assert(points == expected)

  }

}
