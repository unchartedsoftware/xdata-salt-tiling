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

package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.sql.DataFrame
import software.uncharted.salt.xdata.projection.CartesianTimeProjection
import software.uncharted.salt.xdata.util.RangeDescription
import software.uncharted.xdata.spark.SparkFunSpec

// scalastyle:off magic.number

case class CartesianTestData(x: Double, y: Double, value: Double, time: Long)

class CartesianTimeHeatmapTest extends SparkFunSpec {

  private val xCol = "x"
  private val yCol = "y"
  private val timeCol = "time"
  private val value = "value"

  def genData: DataFrame = {

    val testData =
      // 1st time bucket

      List(
        CartesianTestData(0.24, 0.24, 1.0, 101L),
        CartesianTestData(0.6, 0.24, 2.0, 101L),
        CartesianTestData(0.26, 0.26, 3.0, 101L),
        CartesianTestData(0.76, 0.26, 4.0, 101L),
        CartesianTestData(0.24, 0.6, 5.0, 101L),
        CartesianTestData(0.6, 0.6, 6.0, 101L),
        CartesianTestData(0.26, 0.76, 7.0, 101L),
        CartesianTestData(0.76, 0.76, 8.0, 101L),
        // 2nd time bucket
        CartesianTestData(0.24, 0.24, 9.0, 201L),
        CartesianTestData(0.6, 0.24, 10.0, 201L),
        CartesianTestData(0.26, 0.26, 11.0, 201L),
        CartesianTestData(0.76, 0.26, 12.0, 201L),
        CartesianTestData(0.24, 0.6, 13.0, 201L),
        CartesianTestData(0.6, 0.6, 14.0, 201L),
        CartesianTestData(0.26, 0.76, 15.0, 201L),
        CartesianTestData(0.76, 0.76, 16.0, 201L),
        // 3rd time bucket
        CartesianTestData(0.01, 0.99, 0.1, 301L),
        CartesianTestData(0.01, 0.99, 0.1, 301L))

    val tsqlc = sparkSession
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("CartesianTimeHeatmapTest") {
    it("should create a quadtree of tiles where empty tiles are skipped") {
      val result = CartesianTimeHeatmap(xCol, yCol, timeCol, Some(value), Some((0.0, 0.0, 1.0, 1.0)),
        RangeDescription.fromCount(0, 800, 10), (0 to 2), 10)(genData)
        .collect()
        .map(_.coords).
        toSet

      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3), (2, 0, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should create time bins from a range and bucket count") {
      val result = CartesianTimeHeatmap(xCol, yCol, timeCol, Some(value), None, RangeDescription.fromCount(0, 800, 10), (0 until 3), 10)(genData).collect()
      assertResult(10 * 10 * 10)(result(0).bins.length)
    }

    it("should sum values that are in the same bin ") {
      val result = CartesianTimeHeatmap(xCol, yCol, timeCol, Some(value), None, RangeDescription.fromCount(0, 800, 10), Seq(0), 10)(genData).collect()
      val proj = new CartesianTimeProjection(Seq(0), (0.0, 0.0), (1.0, 1.0), RangeDescription.fromCount(0L, 800L, 10))
      assertResult(0.2)(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }

    it("should not aggregate across time buckets") {
      val result = CartesianTimeHeatmap(xCol, yCol, timeCol, None, None, RangeDescription.fromCount(0, 800, 10), (0 until 3), 10)(genData).collect()
      val proj = new CartesianTimeProjection(Seq(0), (0.0, 0.0), (1.0, 1.0), RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(2)(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(2)(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(2)(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }

    it("should use a value of 1.0 for each bin when no value column is specified") {
      val result = CartesianTimeHeatmap(xCol, yCol, timeCol, None, None, RangeDescription.fromCount(0, 800, 10), Seq(0), 10)(genData).collect()
      val proj = new CartesianTimeProjection(Seq(0), (0.0, 0.0), (1.0, 1.0), RangeDescription.fromCount(0L, 800L, 10))
      assertResult(2)(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }
  }
}
