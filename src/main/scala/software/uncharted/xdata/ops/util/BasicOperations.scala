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
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import com.databricks.spark.csv.CsvParser
import org.apache.spark.sql.types.StructType



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



  // Basic RDD oeprations
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



  // Dataframe/RDD conversion functions
  /**
    * Convert an RDD of objects that are products (e.g. case classes) into a dataframe
    *
    * @param sqlc A SQL context into which to set the dataframe
    * @param input The input data
    * @tparam T The type of input data
    * @return A dataframe representing the same data
    */
  def toDataFrame[T <: Product : TypeTag](sqlc: SQLContext)(input: RDD[T]): DataFrame = {
    sqlc.createDataFrame(input)
  }

  /**
    * Convert an RDD of strings into a dataframe
    *
    * @param sqlc A SQL context into which to set the dataframe.
    * @param settings Settings to control CSV parsing.
    * @param schemaOpt The schema into which to parse the data (if null, inferSchema must be true)
    * @param input The input data
    * @return A dataframe representing the same data, but parsed into proper typed rows.
    */
  def toDataFrame (sqlc: SQLContext, settings: Map[String, String], schemaOpt: Option[StructType])(input: RDD[String]): DataFrame = {
    val parser = new CsvParser

    // Move settings to our parser
    def setParserValue (key: String, setFcn: String => Unit): Unit =
      settings.get(key).foreach(strValue => setFcn(strValue))
    def setParserBoolean (key: String, setFcn: Boolean => Unit): Unit =
      setParserValue(key, value => setFcn(value.trim.toLowerCase.toBoolean))
    // scalastyle:off null
    def setParserCharacter (key: String, setFcn: Character => Unit): Unit =
      setParserValue(key, value => setFcn(if (null == value) null else value.charAt(0)))
    // scalastyle:on null

    setParserBoolean("useHeader", parser.withUseHeader(_))
    setParserBoolean("ignoreLeadingWhiteSpace", parser.withIgnoreLeadingWhiteSpace(_))
    setParserBoolean("ignoreTrailingWhiteSpace", parser.withIgnoreTrailingWhiteSpace(_))
    setParserBoolean("treatEmptyValuesAsNull", parser.withTreatEmptyValuesAsNulls(_))
    setParserBoolean("inferSchema", parser.withInferSchema(_))
    setParserCharacter("delimiter", parser.withDelimiter(_))
    setParserCharacter("quote", parser.withQuoteChar(_))
    setParserCharacter("escape", parser.withEscape(_))
    setParserCharacter("comment", parser.withComment(_))
    setParserValue("parseMode", parser.withParseMode(_))
    setParserValue("parserLib", parser.withParserLib(_))
    setParserValue("charset", parser.withCharset(_))
    setParserValue("codec", parser.withCompression(_))

    schemaOpt.map(schema => parser.withSchema(schema))

    parser.csvRdd(sqlc, input)
  }

  def joinDataFrames (indexColumnA: String, indexColumnB: String)(inputA: DataFrame, inputB: DataFrame): DataFrame = {
    inputA.join(inputB, inputA(indexColumnA) === inputB(indexColumnB))
  }
}

class OverlappingSchemaException (val overlappingColumns: Seq[String], message: String, cause: Throwable)
  extends Exception(message, cause) {
  // scalastyle:off null
  def this (overlappingColumns: Seq[String]) = this(overlappingColumns, null, null)
  def this (overlappingColumns: Seq[String], cause: Throwable) = this(overlappingColumns, null, cause)
  def this (overlappingColumns: Seq[String], message: String) = this(overlappingColumns, message, null)
  // scalastyle:on null
}
