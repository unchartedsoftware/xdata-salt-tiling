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

package software.uncharted.salt.contrib.projection.geometry

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.math.{toRadians, sqrt}
import scala.language.implicitConversions

class ArcBinnerTestSuite extends FunSuite {
  private val  clockwise = true
  private val counterclockwise = false

  import ArcBinner._
  import Line.{distance, intPointToDoublePoint}

  val epsilon = 1E-12

  implicit def toDoubleTupleMatcher(values: (Double, Double)): DoubleTupleMatcher = new DoubleTupleMatcher(values, epsilon)

  test("Arc center") {
    getArcCenter((5.0, 0.0), (0.0, 5.0), math.Pi / 2, counterclockwise) shouldBe ((0.0, 0.0) +- epsilon)
    getArcCenter((1.0, 5.0), (6.0, 0.0), math.Pi / 2, clockwise) shouldBe ((1.0, 0.0) +- epsilon)
  }

  test("Arc radius") {
    getArcRadius((10.0, 0.0), (5.0, 5.0), math.Pi / 2) should be(5.0 +- epsilon)
    getArcRadius((-2.0, 7.0), (3.0, 2.0), math.Pi / 2) should be(5.0 +- epsilon)
  }

  test("Octant determination") {
    assert(0 === getOctant(4, 1))
    assert(1 === getOctant(1, 4))
    assert(2 === getOctant(-1, 4))
    assert(3 === getOctant(-4, 1))
    assert(4 === getOctant(-4, -1))
    assert(5 === getOctant(-1, -4))
    assert(6 === getOctant(1, -4))
    assert(7 === getOctant(4, -1))
  }

  test("Octant borderline determination") {
    assert(0 === getOctant(4, 4))
    assert(1 === getOctant(0, 4))
    assert(2 === getOctant(-4, 4))
    assert(3 === getOctant(-4, 0))
    assert(4 === getOctant(-4, -4))
    assert(5 === getOctant(0, -4))
    assert(6 === getOctant(4, -4))
    assert(7 === getOctant(4, 0))
  }

  test("Simple test of full arc, forward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    var last: (Int, Int) = arcBinner.next()
    assert((5, 5) === last)

    while (arcBinner.hasNext) {
      val next = arcBinner.next()
      assert(distance(next, last) < math.sqrt(2) + epsilon)
      assert(distance(next, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      last = next
    }
    assert((-5, 5) === last)
  }

  test("test of iterable return, forward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    val points = arcBinner.remaining.toList
    assert((5, 5) === points.head)
    assert((-5, 5) === points.last)

    points.sliding(2).foreach { pair =>
      val first = pair.head
      val second = pair.last

      assert(distance(first, second) < math.sqrt(2) + epsilon)
      assert(distance(first, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      assert(math.atan2(first._2, first._1) < math.atan2(second._2, second._1))
    }
  }

  test("Simple test of full arc, backward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    arcBinner.resetToEnd()
    var last: (Int, Int) = arcBinner.previous()
    assert((-5, 5) === last)

    while (arcBinner.hasPrevious) {
      val next = arcBinner.previous()
      assert(distance(next, last) < math.sqrt(2) + epsilon)
      assert(distance(next, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      last = next
    }
    assert((5, 5) === last)
  }

  test("test of iterable return, backward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    arcBinner.resetToEnd()
    val points = arcBinner.preceding.toList
    assert((-5, 5) === points.head)
    assert((5, 5) === points.last)

    points.sliding(2).foreach{ pair =>
      val first = pair.head
      val second = pair.last

      assert(distance(first, second) < math.sqrt(2) + epsilon)
      assert(distance(first, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      assert(math.atan2(first._2, first._1) > math.atan2(second._2, second._1))
    }
  }

  test("Test off-origin arc") {
    val binner = new ArcBinner((24, 24), (36, 33), math.Pi/3, clockwise = true)
    val center = ArcBinner.getArcCenter((24, 24), (36, 33), math.Pi/3, clockwise = true)
    val radius = distance(center, (24, 24))

    distance(center, (36, 33)) should be (radius +- epsilon)

    while (binner.hasNext) {
      distance(center, binner.next) should be (radius +- (math.sqrt(2)/2.0 + epsilon))
    }
  }

  test("find the angle of a a given point on a circle around the origin") {
    val deg90 = DoubleTuple(0,1)
    val resultDeg90 = getCircleAngle(deg90)

    val deg45 = DoubleTuple(1, 1)
    val resultDeg45 = getCircleAngle(deg45)

    assertResult(toRadians(90))(resultDeg90)
    assertResult(toRadians(45))(resultDeg45)
  }

  test("DoubleTuple methods") {
    val doubleTuple1 = DoubleTuple(2.2, 3.7)
    val doubleTuple2 = DoubleTuple(4.1, 7.1)
    val doubleTuple3 = DoubleTuple(5.0, 1.5)
    val doubleTuple4 = DoubleTuple(2.5, 0.5)

    val additionResult = doubleTuple1 + doubleTuple2
    val subtractionResult = doubleTuple1 - doubleTuple2
    val multiplicationResult = doubleTuple1 * doubleTuple2
    val divisionResultDoubleTuple = doubleTuple3 / doubleTuple4
    val divsionResultDouble = doubleTuple3 / 0.5
    val length = doubleTuple1.length
    val floor = doubleTuple1.floor
    val ceil = doubleTuple1.ceil

    assertResult(DoubleTuple(6.3, 10.8))(additionResult)
    assertResult(DoubleTuple(2.2-4.1, 3.7-7.1))(subtractionResult)
    assertResult(DoubleTuple(9.02, 26.27))(multiplicationResult)
    assertResult(DoubleTuple(2.0, 3.0))(divisionResultDoubleTuple)
    assertResult(DoubleTuple(10.0, 3.0))(divsionResultDouble)
    assertResult(sqrt(18.53))(length)
    assertResult(DoubleTuple(2, 3))(floor)
    assertResult(DoubleTuple(3, 4))(ceil)

  }

  test("toClosestModulus") {
    assertResult(10)(toClosestModulus(10, 2, 4))
    assertResult(10)(toClosestModulus(10, -2, 4))
    assertResult(8)(toClosestModulus(10, 12, 4))
    assertResult(8)(toClosestModulus(10, 8, 4))
    assertResult(9)(toClosestModulus(10, 9, 4))
    assertResult(9)(toClosestModulus(10, 17, 4))
    assertResult(11)(toClosestModulus(10, 15, 4))
    assertResult(9)(toClosestModulus(10, 12, 3))
  }

  test("DoubleRotation") {
    val doubleRotation = DoubleRotation(0, 1, -1, 0)
    val rotateResult = doubleRotation.rotate(1, 2)

    assert(rotateResult == DoubleTuple(2.0, -1.0))
  }

  test("getInterveningSectors") {
    var startSector = 1
    var withinStartSector = 2.0
    var endSector = 3
    var withinEndSector = 3.5
    var numSectors = 5
    var positive = true
    val result1 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)

    assert(result1 == Seq(1, 2, 3))

    startSector = 5
    val result2 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)

    assert(result2 == Seq(5, 6, 7, 8))

    startSector = 3
    val result3 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result3 == Seq(3, 4, 5, 6, 7, 8))

    withinStartSector = 5
    val result4 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result4 == Seq(3))

    positive = false
    val result5 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result5 == Seq(3, 2, 1, 0, 4, 3))

    endSector = 2
    val result6 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result6 == Seq(3, 2))

    endSector = 5
    val result7 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result7 == Seq(3, 2, 1, 0))

    endSector = 3
    withinStartSector = 2
    val result8 = getInterveningSectors(startSector, withinStartSector, endSector, withinEndSector, numSectors, positive)
    assert(result8 == Seq(3))
  }
}

case class DoubleTupleMatcher (right: DoubleTuple, epsilon: Double = 1E-12) extends BeMatcher[DoubleTuple] {
  def +- (newEpsilon: Double) = DoubleTupleMatcher(right, newEpsilon)

  override def apply (left: DoubleTuple): MatchResult = {
    MatchResult(
      (right.x - left.x).abs < epsilon && (right.y - left.y).abs < epsilon,
      left+" != "+right,
      left+" == "+right
    )
  }
}
