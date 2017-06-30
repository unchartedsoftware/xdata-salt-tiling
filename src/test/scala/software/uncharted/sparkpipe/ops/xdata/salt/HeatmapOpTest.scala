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
import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.salt.xdata.projection.XYTimeProjection
import software.uncharted.sparkpipe.ops.text.util.RangeDescription
import software.uncharted.xdata.spark.SparkFunSpec

// scalastyle:off magic.number

case class HeatmapTestData(x: Double, y: Double, value: Double)

class HeatmapOpTest extends SparkFunSpec {

  private val xCol = "x"
  private val yCol = "y"
  private val value = "value"

  private val baseProjection = new CartesianProjection(0 to 2, (0.0, 0.0), (1.0, 1.0))

  def genData: DataFrame = {

    val testData = List(
        HeatmapTestData(0.24, 0.24, 1.0),
        HeatmapTestData(0.6, 0.24, 2.0),
        HeatmapTestData(0.26, 0.26, 3.0),
        HeatmapTestData(0.76, 0.26, 4.0),
        HeatmapTestData(0.24, 0.6, 5.0),
        HeatmapTestData(0.6, 0.6, 6.0),
        HeatmapTestData(0.26, 0.76, 7.0),
        HeatmapTestData(0.76, 0.76, 8.0))

    val tsqlc = sparkSession
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("HeatmapOpTest") {
    it("should create a quadtree of tiles where empty tiles are skipped") {
      val result = HeatmapOp(xCol, yCol, Some(value), baseProjection, 0 to 2, 10)(genData)
        .collect()
        .map(_.coords).
        toSet

      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should sum values that are in the same bin ") {
      val cartesianProjection = new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0))
      val result = HeatmapOp(xCol, yCol, Some(value), cartesianProjection, Seq(0), 10)(genData).collect()
      assertResult(4.0)(result(0).bins(cartesianProjection.binTo1D((2, 7), (9, 9))))
    }

    it("should use a value of 1.0 for each bin when no value column is specified") {
      val proj = new CartesianProjection(Seq(0), (0.0, 0.0), (1.0, 1.0))
      val result = HeatmapOp(xCol, yCol, None, baseProjection, Seq(0), 10)(genData).collect()
      assertResult(2)(result(0).bins(proj.binTo1D((2, 7), (9, 9))))
    }
  }
}
