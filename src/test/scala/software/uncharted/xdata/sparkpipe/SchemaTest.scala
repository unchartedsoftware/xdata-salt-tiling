/**
 * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted(tm), formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.xdata.sparkpipe

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType,
  LongType, ShortType, StringType, StructField, TimestampType}
import org.scalatest.FunSpec
import software.uncharted.xdata.sparkpipe.config.Schema

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
        assertResult(None)(Schema(config))
      }

      it("should fail on unsupported variable types") {
        val csvSchema = "csvSchema { a { type = blarg, index = 0} }"
        val config = ConfigFactory.parseString(csvSchema)
        assertResult(None)(Schema(config))
      }

      it("should fail on variables that don't have both type and index") {
        val csvSchema = "csvSchema { a { index = 0} } "
        val config = ConfigFactory.parseString(csvSchema)
        assertResult(None)(Schema(config))
      }
    }
  }
}
