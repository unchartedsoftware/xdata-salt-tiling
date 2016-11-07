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

import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

object DataFrameOperations {
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

      // Cast the values of each parsed row to the appropriate type, dropping any that
      // contain bad data.  The createDataFrame call fails if the types in the row
      // don't match the internal types associated with the fields defined in the schema.
      val typedLines = parsedLines
        .map(_.zipWithIndex)
        .map { line =>
          line.map { indexedField =>
            val field = schema.fields(indexedField._2)
            // cast parsed string value to datatype from schema.
            val c = castFromSchema(indexedField._1, field)
            c
          }
        }
      // drop any casts that failed and create a sequence of rows from them
      typedLines.filter(typedLine => typedLine.forall(element => !element.isEmpty))
        .map(t => Row.fromSeq(t.flatten.toSeq))
    }

    // Parse the csv RDD into rows.  This is all wrapped in a closure
    // and run against the partitions to avoid spark serialization errors.
    val rows = input.mapPartitions(p => parse(p))
    sparkSession.createDataFrame(rows, schema)
  }

  // Creates a CSV parser from a settings map.  Attempts to conform to settings available in the
  // spark-csv lib, which was rolled into Spark 2.0+.
  private def createCsvParser(settings: Map[String, String]): CsvParser = {

    def setParserBoolean(key: String, default: Boolean, setFcn: Boolean => Unit): Unit = {
      val value = Option(settings.getOrElse(key, default.toString))
      setFcn(value.exists(_.trim.toLowerCase.toBoolean))
    }

    def setParserCharacter(key: String, default: Character, setFcn: Char => Unit): Unit = {
      val value = Option(settings.getOrElse(key, default.toString))
      setFcn(value.map(s => if (s.isEmpty()) '\u0000' else s.charAt(0)).getOrElse('\u0000'))
    }

    val parserSettings = new CsvParserSettings()
    val parserFormat = new CsvFormat()

    setParserBoolean("ignoreLeadingWhiteSpaces", default = true, parserSettings.setIgnoreLeadingWhitespaces)
    setParserBoolean("ignoreTrailingWhiteSpaces", default = true, parserSettings.setIgnoreTrailingWhitespaces)
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
  private def castFromSchema(value: String, schemaField: StructField): Option[Any]= {
    Try(
      schemaField.dataType.typeName match {
        case "boolean" => Some(value.toBoolean)
        case "byte" => Some(value.toByte)
        case "short" => Some(value.toShort)
        case "integer" => Some(value.toInt)
        case "long" => Some(value.toLong)
        case "float" => Some(value.toFloat)
        case "double" => Some(value.toDouble)
        case "string" => Some(value)
        case "timestamp" => Some(value.toLong)
        case "date" => Some(value.toInt)
        case _ => None
      }
    ).getOrElse(None)
  }
}
