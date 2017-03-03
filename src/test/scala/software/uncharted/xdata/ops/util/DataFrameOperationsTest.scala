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

class DataFrameOperationsTest extends SparkFunSpec {
  import DataFrameOperations._

  describe("#toDataFrame") {
    it("should work with a case class") {
      val data = sc.parallelize(Seq(TestRow(1, 1.0, "one"), TestRow(2, 2.0, "two"), TestRow(3, 3.0, "three"), TestRow(4, 4.0, "four")))
      val converted = toDataFrame(sparkSession)(data)
      assertResult(List(1, 2, 3, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
      assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
      assertResult(List("one", "two", "three", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }

    it("should work with a .csv with an explicit schema") {
      val data = sc.parallelize(Seq("1,1.0,one", "2,2.0,two", "3,3.0,three", "4,4.0,four"))
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
      val converted = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      assertResult(List(1, 2, 3, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
      assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
      assertResult(List("one", "two", "three", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }

    it("should drop .csv rows with incorrect length when working with an explicit schema") {
      val data = sc.parallelize(Seq("1,1.0,one", "2,2.0", "3,3.0,three,three_again", "4,4.0,four"))
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
      val converted = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      assertResult(List(1, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
      assertResult(List(1.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
      assertResult(List("one", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }

    it("should drop .csv rows with data that doesn't match an explicit schema") {
      val data = sc.parallelize(Seq("1,1.0,one", "two,2.0,two", "true,3.0,three", "4,4.0,four"))
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
      val converted = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      assertResult(List(1, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
      assertResult(List(1.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
      assertResult(List("one", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }

    it("should work with a .csv with an explicit schema and a custom settings") {
      val settings = Map(
        "delimiter" -> "*",
        "quote" -> "+",
        "ignoreTrailingWhiteSpaces" -> "false",
        "ignoreLeadingWhiteSpaces" -> "false",
        "comment" -> "null"
      )
      val data = sc.parallelize(Seq("1*1.0*+one*two+", "2*2.0*  two  ", "3*3.0*three", "4*4.0*four"))
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
      val converted = toDataFrame(sparkSession, settings, schema)(data)
      assertResult(List(1, 2, 3, 4))(converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
      assertResult(List(1.0, 2.0, 3.0, 4.0))(converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
      assertResult(List("one*two", "  two  ", "three", "four"))(converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }
  }

  describe("#joinDataFrames") {
    it("Should combine two dataframes correctly") {
      val left = toDataFrame(sparkSession)(sc.parallelize(Seq(TestRow(1, 0.5, "a"), TestRow(2, 1.5, "b"), TestRow(3, 2.5, "c"))))
      val right = toDataFrame(sparkSession)(sc.parallelize(Seq(TestRow(1, 1.0, "e"), TestRow(2, 2.0, "d"), TestRow(3, 3.0, "c"), TestRow(4, 4.0, "b"))))

      val rawJoined = joinDataFrames("a", "a")(left, right).rdd.collect
      val joined = joinDataFrames("a", "a")(left, right).rdd.collect.map { row =>
        assert(row(0) === row(3))
        (
          row(0).asInstanceOf[Int],
          row(1).asInstanceOf[Double], row(2).asInstanceOf[String],
          row(4).asInstanceOf[Double], row(5).asInstanceOf[String]
        )
      }.sortBy(_._1)

      assert(3 === joined.length)
      assert((1, 0.5, "a", 1.0, "e") === joined(0))
      assert((2, 1.5, "b", 2.0, "d") === joined(1))
      assert((3, 2.5, "c", 3.0, "c") === joined(2))
    }
  }
  describe("#rowTermFilterTest") {
    it("should filter out rows if selected text field doesn't contain the input term") {
      val terms = List("cat")
      val testSchema = StructType(Seq(
        StructField("animalType", StringType),
        StructField("petName", StringType))
      )
      val data = sc.parallelize(Seq("dog,choco", "cat,fluffy", "dog,doom", "rabbit,snowy"))
      val inputDF = toDataFrame(sparkSession, Map[String, String](), testSchema)(data)
      val resultDF = rowTermFilter(terms, "animalType")(inputDF)

      //compare result with expected
      assertResult(List("cat"))(resultDF.select("animalType").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }
    it("should filter out rows when selected text field doesn't contain multiple input terms") {
      val terms = List("cat", "mouse")
      val testSchema = StructType(Seq(
        StructField("lineNumber", StringType),
        StructField("sentence", StringType))
      )
      val data = sc.parallelize(Seq("1,The cat was looking for the mouse.", "2,But the mouse was sleeping away in a different corner.", "3,It was a sunny day."))
      val inputDF = toDataFrame(sparkSession, Map[String, String](), testSchema)(data)
      val resultDF = rowTermFilter(terms, "sentence")(inputDF)

      //compare result with expected
      assertResult(List("1", "2"))(resultDF.select("lineNumber").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }
    it("should test rowTermFilter function with regex boundaries considered") {
      val terms = List("cat", "mouse")
      val testSchema = StructType(Seq(
        StructField("lineNumber", StringType),
        StructField("sentence", StringType))
      )
      val data = sc.parallelize(Seq("1,Thecatwaslookingforthemouse.", "2,But the mouse was sleeping away in a different corner.", "3,It was a sunny day."))
      val inputDF = toDataFrame(sparkSession, Map[String, String](), testSchema)(data)
      val resultDF = rowTermFilter(terms, "sentence")(inputDF)

      //compare result with expected
      assertResult(List("2"))(resultDF.select("lineNumber").rdd.map(_(0).asInstanceOf[String]).collect.toList)
    }
  }
}
