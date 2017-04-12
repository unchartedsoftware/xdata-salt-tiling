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
package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.sql.types._
import software.uncharted.sparkpipe.ops.core.{dataframe, rdd}
import software.uncharted.sparkpipe.ops.xdata.salt.BasicSaltOperations.{cartesianTiling, getBounds}
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

    describe("#cartesianTiling") {
      it("should properly tile without autobounds") {
        val data = rdd.toDF(sparkSession)(sc.parallelize(Seq(
          Coordinates(0.0,-1.0, 0.0, 0.0),
          Coordinates(0.0, 0.0, -1.0, 0.0),
          Coordinates(0.0, 0.0, 0.0, 0.0),
          Coordinates(0.0, 0.5, 0.5, 0.0),
          Coordinates(0.0, 1.5, 3.5, 0.0),
          Coordinates(0.0, 2.5, 2.5, 0.0),
          Coordinates(0.0, 3.5, 1.5, 0.0),
          Coordinates(0.0, 4.0, 0.0, 0.0),
          Coordinates(0.0, 0.0, 4.0, 0.0)
        )))
        val tiles = cartesianTiling(
          "x", "y", "count", Seq(0), Some((0.0, 0.0, 4.0, 4.0)), 4)(dataframe.addColumn("count", () => 1)(data)).collect

        assert(List(0.0, 1.0, 0.0, 0.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === tiles(0).bins.seq.toList)
      }

      it("should properly tile with autobounds") {
        val data = rdd.toDF(sparkSession)(sc.parallelize(Seq(
          Coordinates(0.0, 0.0, 0.0, 0.0),
          Coordinates(0.0, 0.5, 0.5, 0.0),
          Coordinates(0.0, 1.5, 3.5, 0.0),
          Coordinates(0.0, 2.5, 2.5, 0.0),
          Coordinates(0.0, 3.5, 1.5, 0.0),
          Coordinates(0.0, 4.0, 4.0, 0.0)
        )))
        val tiles = cartesianTiling(
          "x", "y", "count", Seq(0), None, 4)(dataframe.addColumn("count", () => 1)(data)).collect

        assert(List(0.0, 1.0, 0.0, 1.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === tiles(0).bins.seq.toList)
      }
    }
  }
}

case class Coordinates (w: Double, x: Double, y: Double, z: Double)
