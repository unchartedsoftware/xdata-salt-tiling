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

package software.uncharted.xdata.tiling.config

import com.typesafe.config.Config
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.{SortedMap, immutable}
import scala.util.Try

object Schema {

  case class FieldData(name: String, ftype: String, index: Int)

  case class FieldDataException(message: String) extends Exception(message)

  def apply(config: Config): Try[StructType] = {
    Try {
      // Extract and sort fields
      val fieldNames = config.getObject("csvSchema").keySet().asScala.filterNot(_ == "rowSize")
      val fieldData = fieldNames.map { f =>
        val fieldCfg = config.getConfig("csvSchema").getConfig(f)
        FieldData(f, fieldCfg.getString("type"), fieldCfg.getInt("index"))
      }
      val sortedFields = fieldData.toSeq.sortBy(_.index)

      // Grab the row size if it's there
      val rowSize = if (config.hasPath("csvSchema.rowSize")) Some(config.getInt("csvSchema.rowSize")) else None

      // Make sure there are no duplicate indices
      val duplicateIndices = fieldData.groupBy(_.index)
        .filter(p => p._2.size > 1)
        .map(p => p._2.head.index)
      if (duplicateIndices.nonEmpty) throw FieldDataException(s"Duplicate indices [$duplicateIndices]")

      // Create a contiguous set of fields with stand-in data for unmapped columns
      val fieldMap = sortedFields.map(s => s.index -> s)
      val sortedMap = SortedMap(fieldMap:_*)
      //val contiguousFields = for (i <- 0 until rowSize.getOrElse(sortedMap.lastKey + 1)) yield {
      val contiguousFields = for (i <- 0 until rowSize.getOrElse(sortedMap.lastKey + 1)) yield {
        sortedMap.getOrElse(i, FieldData(s"__unspecified_${i}__", "string", i))
      }

      // Create spark sql field types from contiguous field data
      StructType(createStructFields(contiguousFields))
    }
  }

  // scalastyle:off cyclomatic.complexity
  // disable cyclomatic complexity because it gets tripped up by the legitimately
  // large case statement
  private def createStructFields(contiguousFields: immutable.IndexedSeq[FieldData]): IndexedSeq[StructField] = {
    // Create the schema from the fields
    val structFields = contiguousFields.map { fieldData =>
      fieldData.ftype.toLowerCase match {
        case "boolean" => StructField(fieldData.name, BooleanType)
        case "byte" => StructField(fieldData.name, ByteType)
        case "short" => StructField(fieldData.name, ShortType)
        case "int" => StructField(fieldData.name, IntegerType)
        case "long" => StructField(fieldData.name, LongType)
        case "float" => StructField(fieldData.name, FloatType)
        case "double" => StructField(fieldData.name, DoubleType)
        case "string" => StructField(fieldData.name, StringType)
        case "date" => StructField(fieldData.name, DateType)
        case "timestamp" => StructField(fieldData.name, TimestampType)
        case _ => throw FieldDataException(s"Unhandled variable type ${fieldData.ftype}")
      }
    }
    structFields
  }
  // scalastyle:on cyclomatic.complexity
}
