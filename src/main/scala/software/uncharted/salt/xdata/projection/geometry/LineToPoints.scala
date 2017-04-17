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


/**
  * This class handles converting endpoints to all points in a line.
  *
  * The basis of it is Bressenham's algorithm.
  *
  * Iteration uses Bressenham's algorithm as is, but we also extend it to allow jumps.
  */
class LineToPoints (start: (Int, Int), end: (Int, Int)) {
  import Line._ // scalastyle:ignore
  val totalLength = distance(start, end)

  private val (steep, x0, y0, x1, y1) = {
    val (xs, ys) = start
    val (xe, ye) = end
    val steep = math.abs(ye - ys) > math.abs(xe - xs)

    if (steep) {
      (steep, ys, xs, ye, xe)
    } else {
      (steep, xs, ys, xe, ye)
    }
  }
  private val ystep = if (y0 < y1) 1 else -1
  private val xstep = if (x0 < x1) 1 else -1
  private val deltax = x1 - x0
  private val deltay = math.abs(y1 - y0)
  private var error: Int = 0
  private var y: Int = 0
  private var x: Int = 0
  reset()


  /** Reset this object to start back at the start point of the line */
  def reset(): Unit = {
    error = (deltax * xstep) >> 1
//      if (xstep > 0) deltax >> 1
//      else -deltax - (-deltax >> 1)
    y = y0
    x = x0
  }

  /** Get the current position along the longer axis of the line */
  def curLongAxisPosition: Int = x

  /** Get the last position along the longer axis of the line */
  def lastLongAxisPosition: Int = x1

  /** Get the distance along the longer axis from the start of the line */
  def curLongAxisStartDistance: Int = x - x0

  /** Get the distance along the longer axis from the end of the line */
  def curLongAxisEndDistance: Int  = x1 - x

  /** Get the current distance from the start of the line */
  def curStartDistance: Double = Line.distance((x, y), (x0, y0))

  /** Get the current distance from the end of the line */
  def curEndDistance: Double = Line.distance((x, y), (x1, y1))

  /** Of an arbitrary pair of values, one for x, one for y, get the one that corresponds to the long axis */
  def longAxisValue[T] (values: (T, T)): T =
    if (steep) values._2 else values._1

  /** Of an arbitrary pair of values, one for x, one for y, get the one that corresponds to the short axis */
  def shortAxisValue[T] (values: (T, T)): T =
    if (steep) values._1 else values._2

  /** See if there are more points over which to iterate for this line */
  def hasNext: Boolean = x * xstep <= x1 * xstep

  /** Get the number of points still available in this line */
  def available: Int = (x1 - x) * xstep + 1

  /** Get the current point in this line */
  def current: (Int, Int) = if (steep) (y, x) else (x, y)

  def increasing: Boolean = xstep > 0

  /** Get the next point in this line */
  def next(): (Int, Int) = {
    assert(hasNext)

    val ourY = y
    val ourX = x

    if (xstep > 0) {
      error = error - deltay
      if (error < 0) {
        y = y + ystep
        error = error + deltax
      }
    } else {
      error = error + deltay
      if (error >= -deltax) {
        y = y + ystep
        error = error + deltax
      }
    }
    x = x + xstep

    val ev = (x, y, error)
    if (steep) {
      (ourY, ourX)
    } else {
      (ourX, ourY)
    }
  }

  /**
    * Skip some points, and return the one after those
    *
    * @param n The number of points to skip
    */
  def skip(n: Int): (Int, Int) = {
    if (n > 0) {
      // Convert to long to make sure to avoid overflow
      if (xstep > 0) {
        val errorL = error - deltay * n.toLong
        val ySteps = ((deltax  - 1 - errorL) / deltax).toInt
        error = (errorL + ySteps * deltax ).toInt
        y = y + ySteps * ystep
        x = x + n
      } else {
        val errorL = error + deltay * n.toLong
        val ySteps = (errorL / -deltax).toInt
        error = (errorL + ySteps * deltax).toInt
        y = y + ySteps * ystep
        x = x - n
      }
    }

    next()
  }

  /**
    * Skip some points, and return the one at the specified position on the long axis
    *
    * @param targetX The point along the longer axis of the line to which to skip
    * @return The point in the line with the given long-axis coordinate
    */
  def skipTo(targetX: Int): (Int, Int) = skip((targetX - x) * xstep)

  /**
    * Return all points from the current point until we are farther than a specified distance from a reference point
    *
    * @param from The reference point from which distances are measured
    * @param distance The maximum distance a line point can be from the reference point to be returned
    * @return An iterator that will keep handing back more points until the they are too far from the reference
    *         point.  This iterator <em>will</em> affect this object - i.e., calling next() alternately on the iterator
    *         and the LineToPoint object from which the iterator was obtained will return alternate points on the line.
    */
  def toDistance(from: (Int, Int), distance: Double): Iterator[(Int, Int)] = {
    val referenceDSquared = distance * distance
    val referencePoint =
      if (steep) {
        (from._2, from._1)
      } else {
        from
      }

    new Iterator[(Int, Int)] {
      override def hasNext: Boolean = {
        if (!LineToPoints.this.hasNext) {
          false
        } else {
          val dSquared = Line.distanceSquared((x, y), referencePoint)
          dSquared <= referenceDSquared
        }
      }

      override def next(): (Int, Int) = LineToPoints.this.next()
    }
  }

  /**
    * Skip points until we get to at or within the specified distance from a reference point
    *
    * @param from The reference point from which distances are measured
    * @param distance The maximum distance a line point can be from the reference point to be returned
    * @return The first point from current within the specified distance from the reference point
    */
  def skipToDistance (from: (Int, Int), distance: Double): (Int, Int) = {
    // Get the intersection point of the circle around <code>from</code> of the given radius, and our basic
    // underlying line

    val circle =
      if (steep) {
        Circle(from._2, from._1, distance)
      } else {
        Circle(from._1, from._2, distance)
      }

    val ((ix1, iy1), (ix2, iy2)) = circle.intersection(line)

    val (la1, la2) = (ix1, ix2)

    def before (lhs: Double, rhs: Double) = lhs * xstep < rhs * xstep
    def skipUpTo (n: Double) = if (increasing) skipTo(n.floor.toInt) else skipTo(n.ceil.toInt)

    if (before(la1, x) && before(la2, x)) {
      throw new NoIntersectionException("circle doesn't intersect plane after current point")
    } else if (before(la1, x)) {
      skipUpTo(la2)
    } else if (before(la2, x)) {
      skipUpTo(la1)
    } else if (before(la1, la2)) {
      skipUpTo(la1)
    } else {
      skipUpTo(la2)
    }
  }

  // Our line can be written either as Ax + By = 1 or Ax + By = 0
  // Doing so requires the following values of A and B:
  private lazy val line = Line((x0, y0), (x1, y1))

  /** Get the next N points in this line */
  def next(n: Int): Array[(Int, Int)] = {
    assert(n <= available)

    val result = new Array[(Int, Int)](n)
    for (i <- 0 until n) result(i) = next()

    result
  }

  /** Get the rest of the points in this line */
  def rest(): Array[(Int, Int)] = next(available)
}
