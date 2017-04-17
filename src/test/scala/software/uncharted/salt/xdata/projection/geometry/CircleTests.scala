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

package software.uncharted.salt.xdata.projection.geometry

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.Set



class CircleTests extends FunSuite {
  private val epsilon = 1E-12

  test("Distance to line") {
    Circle((0, 0), 3).distanceTo(Line(1, 2, 10)) should be ((math.sqrt(20) - 3) +- epsilon)
    Circle((0, 0), 4).distanceTo(Line(1, 2, 10)) should be ((math.sqrt(20) - 4) +- epsilon)
    Circle((0, 0), 5).distanceTo(Line(1, 2, 10)) should be (0.0)
  }

  test("line intersection") {
    assert(Set((6.0, 8.0), (6.0, -8.0)) === Circle((0, 0), 10).intersection(Line(1, 0, 6)).productIterator.toSet)
    assert(Set((2.0, 4.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 1, 6)).productIterator.toSet)
    assert(Set((2.0, 0.0), (0.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 1, 2)).productIterator.toSet)
    assert(Set((0.0, 2.0), (2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, -1, -2)).productIterator.toSet)
    assert(Set((2.0, 0.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, -1, 2)).productIterator.toSet)

    assert(Set((2.0, 0.0), (2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 2)).productIterator.toSet)
    assert(Set((0.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 0)).productIterator.toSet)
    assert(Set((4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 4)).productIterator.toSet)

    assert(Set((0.0, 2.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 2)).productIterator.toSet)
    assert(Set((2.0, 0.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 0)).productIterator.toSet)
    assert(Set((2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 4)).productIterator.toSet)
  }
}
