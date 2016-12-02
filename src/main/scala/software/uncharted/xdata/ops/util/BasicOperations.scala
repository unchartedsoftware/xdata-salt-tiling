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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.ClassTag



/**
  * Some basic operations to facilitate easy use of pipelines
  */
object BasicOperations {
  // Basic operations that don't care about pipeline content type
  /**
    * A function to allow optional application within a pipe, as long as there is no type change involved
    *
    * @param optOp An optional operation; if it is defined, it is applied to the input.  If it is not defined, the
    *              input is passed through untouched.
    * @param input The input data
    * @tparam T The type of input data
    * @return The input data transformed by the given operation, if there is such an operation, or else the input
    *         data itself, if not.
    */
  def optional[T] (optOp: Option[T => T])(input: T): T =
    optOp.map(op => op(input)).getOrElse(input)



  // Basic RDD operations
  /**
    * Filter input based on a given test
    *
    * @param test The test to perform
    * @param input The input data
    * @tparam T The type of input data
    * @return The input data that matches the given test
    */
  def filter[T](test: T => Boolean)(input: RDD[T]): RDD[T] =
    input.filter(test)

  /**
    * Filter an input string RDD to only those elements that match (or don't match) a given regular expression
    *
    * @param regexStr The regular expression to match
    * @param exclude If true, filter matching entries out of the RDD; if false, filter out non-matching entries
    * @param input The input data
    * @return The input data, filtered as above.
    */
  def regexFilter (regexStr: String, exclude: Boolean = false)(input: RDD[String]): RDD[String] = {
    val regex = regexStr.r
    input.filter {
      case regex(_*) => if (exclude) false else true
      case _ => if (exclude) true else false
    }
  }

  /**
    * Just map the input data to a new form (use rdd.map, but in a pipeline)
    *
    * @param fcn The transformation function
    * @param input The input data
    * @tparam S The input type
    * @tparam T The output type
    * @return The input data, transformed
    */
  def map[S, T: ClassTag](fcn: S => T)(input: RDD[S]): RDD[T] = input.map(fcn)

  // Basic Dataframe operations
  def filterA (condition: Column)(input: DataFrame): DataFrame =
    input.filter(condition)
}
