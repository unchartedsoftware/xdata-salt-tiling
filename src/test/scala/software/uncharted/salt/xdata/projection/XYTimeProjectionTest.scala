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

import org.scalatest.FunSpec
import software.uncharted.salt.core.projection.numeric.CartesianProjection

// scalastyle:off magic.number
class XYTimeProjectionTest extends FunSpec {
  describe("CartesianTimeProjectionTest") {
    describe("#project()") {
      it("should return None when the data space coordinate is none") {
        val proj = new XYTimeProjection(Long.MinValue, Long.MaxValue, 1, new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0)))
        assertResult(None)(proj.project(None, (32, 32, 32)))
      }

      it("should return None when the time coord is out of range") {
        val proj = new XYTimeProjection(0L, 100L, 10, new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0)))
        assertResult(None)(proj.project(Some(0.0, 0.0, 101L), (32, 32, 32)))
      }

      it("should return None when the coords are out of range") {
        val proj = new XYTimeProjection(0L, 100L, 10, new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0)))
        assertResult(None)(proj.project(Some(0.0, -1.0, 10L), (32, 32, 32)))
      }

      it("should assign values to the correct time bucket") {
        val proj = new XYTimeProjection(10L, 210L, 10, new CartesianProjection(Seq(0), (0.0, 0.0), (200.0, 200.0)))
        assertResult(Some(List(((0, 0, 0), (0, 100, 2)))))(proj.project(Some(0.0, 0.0, 53L), (100, 100, 10)))
      }
    }

    describe("#binTo1D()") {
      it("should convert an (x, y, time) tuple into a linear coordinate") {
        val proj = new XYTimeProjection(Long.MinValue, Long.MaxValue, 1, new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0)))
        assertResult(8064)(proj.binTo1D((4, 8, 12), (30, 20, 10)))
      }
    }

    describe("#binFrom1D()") {
      it("should convert a linear coordinate into an (x, y, time) triple") {
        val proj = new XYTimeProjection(Long.MinValue, Long.MaxValue, 1, new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0)))
        assertResult((4, 8, 12))(proj.binFrom1D(8064, (30, 20, 10)))
      }
    }
  }
}
