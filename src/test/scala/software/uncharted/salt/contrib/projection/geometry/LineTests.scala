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
import scala.collection.Set

class LineTests extends FunSuite {
  private val epsilon = 1E-12

  test("hashCode") {
    val line = Line(1, 0, 0)
    val hashCode = line.hashCode
    assertResult(0)(hashCode)
  }

  test("Line equality") {
    assert(Line(2, 2, 2) === Line(1, 1, 1))

    val line1 = Line(2, 0, 0)
    assertResult(true)(line1 equals Line(2, 0, 0))

    val line2 = Line(2, 2, 0)
    assertResult(true)(line2.equals(Line(2, 2, 0)))

    val line3 = Line(1, 0, 2)
    assertResult(true)(line3.equals(Line(1, 0, 2)))

    assertResult(false)(Line(2, 2, 2) equals 0.0)
  }

  test("approximately equals functionality") {
    val line = Line(0, 0, 0)
    val epsilon = 3

    val result1 = (line~=(epsilon))(line)
    assertResult(true)(result1)

    val result2 = (line~=(epsilon))(Line(0, 0, 2))
    assertResult(true)(result2)

    val result3 = (Line(0, 5, 1)~=(epsilon))(Line(1, 1, 1))
    assertResult(true)(result3)

    // division by zero results in NaN, numeric operation involving NaN results in false
    val result4 = (Line(0, 5, 1)~=(epsilon))(line)
    assertResult(false)(result4)

    val result5 =(Line(0, 1, 1)~=(0.5))(Line(0, 0.5, 0.5))
    assertResult(true)(result5)
  }

  test("yOf and xOf functions") {
    val line1 = Line(0, 0, 0)

    val result1 = line1.yOf(123)
    assert(result1.isNaN)

    val result2 = line1.xOf(123)
    assert(result2.isNaN)

    val line2 = Line(-1, 1, 3)

    assertResult(3)(line2.yOf(0))
    assertResult(1)(line2.yOf(-2))
    assertResult(-3)(line2.xOf(0))
  }

  test("Line construction from points") {
    for (x0 <- -5 to 5; y0 <- -5 to 5; x1 <- -5 to 5; y1 <- -5 to 5) {
      if (x0 == x1 && y0 == y1) {
        intercept[IllegalArgumentException] {
          Line((x0, y0), (x1, y1))
        }
      } else {
        val message = "Line [%d, %d] => [%d, %d]".format(x0, y0, x1, y1)
        val line = Line((x0, y0), (x1, y1))
        if (y0 != y1) {
          line.xOf(y0) should be(x0 * 1.0 +- epsilon)
          line.xOf(y1) should be(x1 * 1.0 +- epsilon)
          line.xOf((y0 + y1) / 2.0) should be((x0 + x1) / 2.0 +- epsilon)
        }
        if (x0 != x1) {
          line.yOf(x0) should be(y0 * 1.0 +- epsilon)
          line.yOf(x1) should be(y1 * 1.0 +- epsilon)
          line.yOf((x0 + x1) / 2.0) should be((y0 + y1) / 2.0 +- epsilon)
        }
      }
    }
  }

  test("Intersection") {
    (Line(1, 2, 3) intersection Line(1, 3, 5)) should be (-1.0, 2.0)
    (Line(0, 1, 5) intersection Line(1, 0, 3)) should be (3.0, 5.0)
    (Line(0, 1, 0) intersection Line(5, 2, 3)) should be (0.6, 0.0)
    (Line(1, 0, 0) intersection Line(4, 10, 7)) should be (0.0, 0.7)

    assert(Set((6.0, 8.0), (6.0, -8.0)) === Line(1, 0, 6).intersection(Circle((0, 0), 10)).productIterator.toSet)
    assert(Set((2.0, 4.0), (4.0, 2.0)) === Line(1, 1, 6).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((2.0, 0.0), (0.0, 2.0)) === Line(1, 1, 2).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((0.0, 2.0), (2.0, 4.0)) === Line(1, -1, -2).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((2.0, 0.0), (4.0, 2.0)) === Line(1, -1, 2).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((2.0, 0.0), (2.0, 4.0)) === Line(1, 0, 2).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((0.0, 2.0)) === Line(1, 0, 0).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((4.0, 2.0)) === Line(1, 0, 4).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)

    assert(Set((0.0, 2.0), (4.0, 2.0)) === Line(0, 1, 2).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((2.0, 0.0)) === Line(0, 1, 0).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)
    assert(Set((2.0, 4.0)) === Line(0, 1, 4).intersection(Circle(2.0, 2.0, 2.0)).productIterator.toSet)

    val caughtMsg = intercept[NoIntersectionException] {
      (Line(1, 2, 3) intersection Line(1, 2, 3))
    }
    assert(caughtMsg.getMessage === "Lines are parallel")
  }

  test("Distance") {
    Line(1, 0, 13).distanceTo(21.0, -4.0) should be (8.0 +- epsilon)
    Line(1, 0, 13).distanceTo( 5.0, -4.0) should be (8.0 +- epsilon)
    Line(0, 1, -3).distanceTo(21.0, -4.0) should be (1.0 +- epsilon)
    Line(0, 1, -3).distanceTo(21.0, -2.0) should be (1.0 +- epsilon)
    Line(1, 2, 10).distanceTo( 0.0,  0.0) should be (math.sqrt(20) +- epsilon)

    Line(1, 2, 10).distanceTo(Circle((0, 0), 3)) should be ((math.sqrt(20) - 3) +- epsilon)
    Line(1, 2, 10).distanceTo(Circle((0, 0), 4)) should be ((math.sqrt(20) - 4) +- epsilon)
    Line(1, 2, 10).distanceTo(Circle((0, 0), 5)) should be (0.0)
  }

}
