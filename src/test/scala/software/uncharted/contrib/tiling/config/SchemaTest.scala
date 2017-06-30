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

package software.uncharted.contrib.tiling.config

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, TimestampType}
import org.scalatest.FunSpec

// scalastyle:off multiple.string.literals
class SchemaTest extends FunSpec {
  describe("SchemaTest") {
    describe("#apply") {
      it("should create a schema object from a Java Properties instance") {
        val csvSchema = """
                       |csvSchema {
                       | a { type = boolean, index = 0}
                       | b { type = byte, index = 1}
                       | c { type = int, index = 2}
                       | d { type = short, index = 3}
                       | e { type = long, index = 4}
                       | f { type = float, index = 5}
                       | g { type = double, index = 6}
                       | h { type = string, index = 7}
                       | i { type = date, index = 8}
                       | j { type = timestamp, index: 9}
                       |}
                     """.stripMargin

        val config = ConfigFactory.parseString(csvSchema)
        val schema = Schema(config).getOrElse(fail())

        info(schema.treeString)

        assertResult(StructField("a", BooleanType))(schema("a"))
        assertResult(StructField("b", ByteType))(schema("b"))
        assertResult(StructField("c", IntegerType))(schema("c"))
        assertResult(StructField("d", ShortType))(schema("d"))
        assertResult(StructField("e", LongType))(schema("e"))
        assertResult(StructField("f", FloatType))(schema("f"))
        assertResult(StructField("g", DoubleType))(schema("g"))
        assertResult(StructField("h", StringType))(schema("h"))
        assertResult(StructField("i", DateType))(schema("i"))
        assertResult(StructField("j", TimestampType))(schema("j"))
      }

      it("should fill in unspecified columns with suitable defaults") {
        val csvSchema = """
                       |csvSchema {
											 | rowSize = 6
                       | a { type = boolean, index = 0}
                       | b { type = byte, index = 2}
                       | c { type = int, index = 4}
                       |}
                     """.stripMargin

        val config = ConfigFactory.parseString(csvSchema)
        val schema = Schema(config).getOrElse(fail())
        info(schema.treeString)

        assertResult(StructField("a", BooleanType))(schema("a"))
        assertResult(StructField("__unspecified_1__", StringType))(schema("__unspecified_1__"))
        assertResult(StructField("b", ByteType))(schema("b"))
        assertResult(StructField("__unspecified_3__", StringType))(schema("__unspecified_3__"))
        assertResult(StructField("c", IntegerType))(schema("c"))
        assertResult(StructField("__unspecified_5__", StringType))(schema("__unspecified_5__"))
      }

      it("should fail on duplicate variable indices") {
        val csvSchema = """
                       |csvSchema {
                       | a { type = boolean, index = 1}
                       | b { type = byte, index = 1}
                       |}
                     """.stripMargin
        val config = ConfigFactory.parseString(csvSchema)
        assert(Schema(config).isFailure)
      }

      it("should fail on unsupported variable types") {
        val csvSchema = "csvSchema { a { type = blarg, index = 0} }"
        val config = ConfigFactory.parseString(csvSchema)
        assert(Schema(config).isFailure)
      }

      it("should fail on variables that don't have both type and index") {
        val csvSchema = "csvSchema { a { index = 0} } "
        val config = ConfigFactory.parseString(csvSchema)
        assert(Schema(config).isFailure)
      }
    }
  }
}
