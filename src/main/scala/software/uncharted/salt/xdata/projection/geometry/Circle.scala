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
  * A circle of the form (x - x0)<sup>2</sup> + (y - y0)<sup>2</sup> = R<sup>2</sup>
  */
case class Circle (center: (Double, Double), radius: Double) {
  // Get the distance to a line from the closest point on this circle
  def distanceTo (line: Line): Double = (0.0 max (line.distanceTo(center._1, center._2) - radius))

  // Line is A x + B y = C
  // Circle is (x - xf)^2 + (y - yf)^2 = r^2
  // solve:
  //    x^2 - 2 x xf + xf^2 + y^2 - 2 y yf + yf^2 = r^2
  //    y = (C - A x) / B
  // or (if B = 0)
  //    x = (C - B y) / A
  //
  // if |B| >>  0
  //    y = (C - A x) / B = C/B - A/B x
  //    (x - xf)^2 + (C/B - A/B x - yf)^2 = r^2
  //    (x - xf)^2 + (C/B - yf - A/B x)^2 = r^2
  //    x^2 - 2 x xf + xf^2 + (C/B - yf)^2 - 2 A/B (C/B - yf) x + (A/B)^2 x^2 = r^2
  //    (1 + (A/B)^2) x^2 - 2 (xf + A/B(C/B - yf)) x + xf^2 + (C/B - yf)^2 - r^2 = 0
  //
  // if |B| ~= 0
  //    x = (C - B y) / A = C/A - B/A y
  //    (C/A - B/A y - xf)^2 + (y - yf)^2 = r^2
  //    (y - yf)^2 + (C/A - xf - B/A y)^2 = r^2
  //    y^2 - 2 yf y + yf^2 + (C/A - xf)^2 - 2 B/A (C/A - xf) y + (B/A)^2 y^2 = r^2
  //    (1 + (B/A)^2) y^2 - 2 (yf + B/A (C/A - xf)) y + yf^2 + (C/A - xf)^2 - r^2 = 0
  def intersection (line: Line): ((Double, Double), (Double, Double)) = {
    val A = line.A
    val B = line.B
    val C = line.C
    val (xf, yf) = center
    val r = radius

    if (A.abs > B.abs) {
      val BoverA = B / A
      val E = C / A - xf
      val a = 1 + BoverA * BoverA
      val b = -2 * (yf + BoverA * E)
      val c = yf * yf + E * E - r * r

      val determinate = math.sqrt(b * b - 4 * a * c)
      if (determinate.isNaN) throw new NoIntersectionException("Circle doesn't intersect line in the real plane (case A > B)")
      val iy1 = (-b + determinate) / (2 * a)
      val ix1 = (C - B * iy1) / A
      val iy2 = (-b - determinate) / (2 * a)
      val ix2 = (C - B * iy2) / A

      ((ix1, iy1), (ix2, iy2))
    } else {
      val AoverB = A / B
      val E = C / B - yf
      val a = 1 + AoverB * AoverB
      val b = -2 * (xf + AoverB * E)
      val c = xf * xf + E * E - r * r

      val determinate = math.sqrt(b * b - 4 * a * c)
      if (determinate.isNaN) throw new NoIntersectionException("Circle doesn't intersect line in the real plane (case B > A)")
      val ix1 = (-b + determinate) / (2 * a)
      val iy1 = (C - A * ix1) / B
      val ix2 = (-b - determinate) / (2 * a)
      val iy2 = (C - A * ix2) / B

      ((ix1, iy1), (ix2, iy2))
    }
  }
}
object Circle {
  def apply (x: Int, y: Int, radius: Double): Circle = Circle((x.toDouble, y.toDouble), radius)
  def apply (x: Double, y: Double, radius: Double): Circle = Circle((x, y), radius)
}
