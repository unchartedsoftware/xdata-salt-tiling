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

import org.apache.spark.sql.types._
import software.uncharted.sparkpipe.ops.core.rdd
import software.uncharted.sparkpipe.ops.xdata.salt.BasicSaltOperations.getBounds
import software.uncharted.sparkpipe.ops.xdata.util.DataFrameOperations.toDataFrame
import software.uncharted.xdata.spark.SparkFunSpec

class BasicSaltOperationsTest extends SparkFunSpec {
  describe("BasicSaltOperations") {
    describe("#getBounds") {
      it("should return the proper bounds of a set of coordinates") {
        val data = rdd.toDF(sparkSession)(sc.parallelize(Seq(
          Coordinates(0.0, 0.0, 0.0, 0.0),
          Coordinates(1.0, 4.0, 3.0, 5.0),
          Coordinates(2.0, 2.0, 1.0, 0.0),
          Coordinates(-1.0, -2.0, -3.0, -4.0)
        )))

        assert(List((-1.0, 2.0), (-2.0, 4.0)) === getBounds("w", "x")(data).toList)
        assert(List((-3.0, 3.0), (-4.0, 5.0)) === getBounds("y", "z")(data).toList)
        assert(List((-2.0, 4.0), (-3.0, 3.0)) === getBounds("x", "y")(data).toList)
      }

      it("should convert float values to double") {
        val rdd = sc.parallelize(Seq(
          "1.0f, 4.0f, 3.0f, 5.0f",
          "2f, 2f, 1f, 0f",
          "-1f, -2f, -3f, -4f"))

        val schema = StructType(Seq(StructField("w", FloatType), StructField("x", FloatType), StructField("y", FloatType), StructField("z", FloatType)))
        val converted = toDataFrame(sparkSession, Map[String, String](), schema)(rdd)

        val result =  getBounds("w", "x")(converted).toList
        val expected = List((-1.0, 2.0), (-2.0, 4.0))

        assertResult(expected)(result)
      }

      it("should convert long values to double") {
        val rdd = sc.parallelize(Seq(
          "1, 4, 3, 5",
          "2, 2, 1, 0",
          "-1, -2, -3, -4"))

        val schema = StructType(Seq(StructField("w", LongType), StructField("x", LongType), StructField("y", LongType), StructField("z", LongType)))
        val converted = toDataFrame(sparkSession, Map[String, String](), schema)(rdd)

        val result =  getBounds("y", "z")(converted).toList
        val expected = List((-3.0, 3.0), (-4.0, 5.0))
        assertResult(expected)(result)
      }

      it("should convert int values to double") {
        val rdd = sc.parallelize(Seq(
          "1, 4, 3, 5",
          "2, 2, 1, 0",
          "-1, -2, -3, -4"))

        val schema = StructType(Seq(StructField("w", IntegerType), StructField("x", IntegerType), StructField("y", IntegerType), StructField("z", IntegerType)))
        val converted = toDataFrame(sparkSession, Map[String, String](), schema)(rdd)

        val result =  getBounds("x", "y")(converted).toList
        val expected = List((-2.0, 4.0), (-3.0, 3.0))
        assertResult(expected)(result)
      }
    }
  }
}

case class Coordinates (w: Double, x: Double, y: Double, z: Double)
