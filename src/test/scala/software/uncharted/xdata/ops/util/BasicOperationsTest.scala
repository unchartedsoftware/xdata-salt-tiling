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
package software.uncharted.xdata.ops.util

import org.apache.spark.sql.types._
import software.uncharted.xdata.spark.SparkFunSpec


class BasicOperationsTest extends SparkFunSpec {
  import BasicOperations._

  describe("BasicOperationsTest") {
    describe("#optional") {
      it("Should transform input if given a transformation") {
        val transform1: Option[Int => Int] = Some((n: Int) => n * n)
        assertResult(16)(optional(transform1)(4))
      }

      it("Should not transform input if not given a transformation") {
        val transform2: Option[Int => Int] = None
        assertResult(4)(optional(transform2)(4))
      }
    }

    describe("#filter") {
      it("Should filter in only numbers that pass its filter") {
        val data = sc.parallelize(1 to 20)
        assertResult(List(3, 8, 13, 18))(filter[Int](n => (3 == (n % 5)))(data).collect.toList)
      }
    }

    describe("#regexFilter") {
      it("SHould pass through only strings that match the given regular expression") {
        val data = sc.parallelize(Seq("abc def ghi", "abc d e f ghi", "the def quick", "def ghi", "abc def"))
        assertResult(List("abc def ghi", "the def quick", "def ghi"))(regexFilter(".*def.+")(data).collect.toList)
        assertResult(List("abc d e f ghi", "def ghi"))(regexFilter(".+def.*", true)(data).collect.toList)
      }
    }

    describe("#map") {
      it("Should transform the input data as described") {
        val data = sc.parallelize(1 to 4)
        assertResult(List(1, 4, 9, 16))(map((n: Int) => n * n)(data).collect.toList)
      }
    }

    describe("#toDataFrame") {
      it("should work with a case class") {
        val data = sc.parallelize(Seq(TestRow(1, 1.0, "one"), TestRow(2, 2.0, "two"), TestRow(3, 3.0, "three"), TestRow(4, 4.0, "four")))
        val converted = toDataFrame(sparkSession)(data)
        assertResult(List(1, 2, 3, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
        assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
        assertResult(List("one", "two", "three", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
      }
// TODO: This should be uncommented when dependency for databricks can be dropped
//      it("should work with a .csv with auto-schema") {
//        val data = sc.parallelize(Seq("1,1.0,one", "2,2.0,two", "3,3.0,three", "4,4.0,four"))
//        val converted = toDataFrame(sparkSession, Map("inferSchema" -> "true"), None)(data)
//        assertResult(List(1, 2, 3, 4))(converted.select("C0").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
//        assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("C1").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
//        assertResult(List("one", "two", "three", "four"))(converted.select("C2").rdd.map(_(0).asInstanceOf[String]).collect.toList)
//      }
//
//      it("should work with a .csv with an explicit schema") {
//        val data = sc.parallelize(Seq("1,1.0,one", "2,2.0,two", "3,3.0,three", "4,4.0,four"))
//        val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
//        val converted = toDataFrame(sparkSession, Map[String, String](), Some(schema))(data)
//        assertResult(List(1, 2, 3, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
//        assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
//        assertResult(List("one", "two", "three", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
//      }
    }
  }
}

case class TestRow (a: Int, b: Double, c: String)
