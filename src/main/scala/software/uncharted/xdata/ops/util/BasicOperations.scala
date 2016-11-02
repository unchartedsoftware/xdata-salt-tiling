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

import com.univocity.parsers.csv
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.types.{StructField, StructType}



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
    * @param sparkSession A spark session into which to set the dataframe
    * @param input The input data
    * @tparam T The type of input data
    * @return A dataframe representing the same data
    */
  def toDataFrame[T <: Product : TypeTag](sparkSession: SparkSession)(input: RDD[T]): DataFrame = {
    sparkSession.createDataFrame(input)
  }


  /**
    * Convert an RDD of strings into a dataframe.
    *
    * @param sparkSession A spark session into which to set the dataframe.
    * @param settings Settings to control CSV parsing.
    * @param schema The schema into which to parse the data
    * @param input The input data
    * @return A dataframe representing the same data, but parsed into proper typed rows.
    */
  def toDataFrame(sparkSession: SparkSession, settings: Map[String, String], schema: StructType)(input: RDD[String]): DataFrame = {
    // The spark-csv library originally supported creating a dataframe from an RDD of CSV strings, but
    // the functionality was removed when spark-csv was incorporated into Spark 2.0.  We provide our own
    // implementation as a work around, but it is currently not as fully featured.
    val parse: (Iterator[String]) => Iterator[Row] = rowStrings => {
      val csvParser = createCsvParser(settings)

      // Parse each line in our RDD, yielding each as an array of tokens.  Any line
      // with an unexpected number of values is dropped.
      val parsedLines = rowStrings.map(str => csvParser.parseLine(str))
        .filter(s => s.length == schema.fields.length)

      // Cast the values of each parsed row to the appropriate type.  The createDataFrame call fails if the
      // the types in the row don't match the internal types associated with the fields defined in the schema.
      parsedLines.map(_.zipWithIndex)
        .map { line =>
          val typedLine = line.map { t =>
            val field = schema.fields(t._2)
            // cast parsed string value to datatype from
            castFromSchema(t._1, field)
          }
          Row.fromSeq(typedLine)
        }
    }

    val rows = input.mapPartitions(p => parse(p))
    sparkSession.createDataFrame(rows, schema)
  }

  // Creates a CSV parser from a settings map.  Attempts to conform to settings available in the
  // spark-csv lib, which was rolled into Spark 2.0+.
  private def createCsvParser(settings: Map[String, String]): CsvParser = {

    def setParserBoolean(key: String, default: Boolean, setFcn: Boolean => Unit): Unit = {
      val value = settings.getOrElse(key, default.toString)
      setFcn(value.trim.toLowerCase.toBoolean)
    }

    def setParserCharacter(key: String, default: Character, setFcn: Char => Unit): Unit = {
      val value = settings.getOrElse(key, default.toString)
      setFcn(value.charAt(0))
    }

    val parserSettings = new CsvParserSettings()
    val parserFormat = new CsvFormat()

    setParserBoolean("ignoreLeadingWhiteSpace", default = true, parserSettings.setIgnoreLeadingWhitespaces)
    setParserBoolean("ignoreTrailingWhiteSpace", default = true, parserSettings.setIgnoreTrailingWhitespaces)
    setParserCharacter("delimiter", ',', parserFormat.setDelimiter)
    setParserCharacter("quote", '\"', parserFormat.setQuote)
    setParserCharacter("escape", '\\', parserFormat.setQuoteEscape)
    setParserCharacter("comment", '\u0000', parserFormat.setComment)
    setParserBoolean("useHeader", default = false, parserSettings.setHeaderExtractionEnabled)

    parserSettings.setFormat(parserFormat)
    new CsvParser(parserSettings)
  }

  // scalastyle:off cyclomatic.complexity
  // Casts a value to the type associated with its corresponding schema entry.
  private def castFromSchema(value: String, schemaField: StructField): Any = {
    schemaField.dataType.typeName match {
      case "boolean" => value.toBoolean
      case "byte" => value.toByte
      case "short" => value.toShort
      case "integer" => value.toInt
      case "long" => value.toLong
      case "float" => value.toFloat
      case "double" => value.toDouble
      case "string" => value
      case "timestamp" => value.toLong
      case "date" => value.toInt
      case _ => ""
    }
  }
}
