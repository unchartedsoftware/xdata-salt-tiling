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
import software.uncharted.sparkpipe.ops.core.rdd
import software.uncharted.xdata.spark.SparkFunSpec

class DataFrameOperationsTest extends SparkFunSpec {
  import DataFrameOperations._

  describe("#toDataFrame") {
    it("should work with a case class") {
      val data = sc.parallelize(Seq(TestRow(1, 1.0, "one"), TestRow(2, 2.0, "two"), TestRow(3, 3.0, "three"), TestRow(4, 4.0, "four")))
      val converted = rdd.toDF(sparkSession)(data)
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

    it ("should correctly castFromSchema") {
      val data = sc.parallelize(Seq("1,true,123,1.0,104,1970-01-18 00:59:38,1970-01-18",
                                    "2,false,321,2.0,111,2017-03-24 15:15:38,2017-03-24"))
      val schema = StructType(Seq(StructField("longColumn", LongType),
                                  StructField("boolColumn", BooleanType),
                                  StructField("shortColumn", ShortType),
                                  StructField("floatColumn", FloatType),
                                  StructField("byteColumn", ByteType),
                                  StructField("timeColumn", TimestampType),
                                  StructField("dateColumn", DateType)))
      val converted = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      val types = converted.dtypes
      assertResult(2)(converted.count())

      assertResult(("longColumn", "LongType"))(types(0))
      assertResult(("boolColumn", "BooleanType"))(types(1))
      assertResult(("shortColumn", "ShortType"))(types(2))
      assertResult(("floatColumn", "FloatType"))(types(3))
      assertResult(("byteColumn", "ByteType"))(types(4))
      assertResult(("timeColumn", "TimestampType"))(types(5))
      assertResult(("dateColumn", "DateType"))(types(6))

      //testing wildcard case in match statement
      val data2 = sc.parallelize(Seq("2,true"))
      val schema2 = StructType(Seq(StructField("intCol", IntegerType),StructField("nullCol", NullType)))
      val converted2 = toDataFrame(sparkSession, Map[String, String](), schema2)(data2)
      assertResult(List())(converted2.select("intCol").rdd.map(_(0).asInstanceOf[String]).collect.toList)

    }
  }
}
