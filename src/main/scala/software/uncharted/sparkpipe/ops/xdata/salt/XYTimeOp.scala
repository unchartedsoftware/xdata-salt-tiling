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

package software.uncharted.sparkpipe.ops.xdata.salt

import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.xdata.projection.XYTimeProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns
import software.uncharted.sparkpipe.ops.xdata.text.util.RangeDescription

trait XYTimeOp {

  val defaultTileSize = 256

  def apply[T, U, V, W, X](// scalastyle:ignore
                           xCol: String,
                           yCol: String,
                           rangeCol: String,
                           timeRange: RangeDescription[Long],
                           valueExtractor: (Row) => Option[T],
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]],
                           zoomLevels: Seq[Int],
                           tileSize: Int,
                           projection: XYTimeProjection)
                          (request: TileRequest[(Int, Int, Int)])(input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int, Int), V, X]] = {

    // Optionally applies a udf to handle conversion from timestamp / date to long, or pass the
    // dataframe through if the value is anything else. We can't use the spark sql cast to long
    // functions because they were modified to return seconds rather than milliseconds in Spark 1.6+.
    // See https://issues.apache.org/jira/browse/SPARK-13341.
    def convertTimes(df: DataFrame) = {
      val conversionUdf = input.schema(rangeCol).dataType match {
        case ts: TimestampType => Some(udf( (t: Timestamp) => t.getTime))
        case dt: DateType => Some(udf( (d: Date) => d.getTime))
        case _ => None
      }
      conversionUdf.map(cudf => input.withColumn(rangeCol, cudf(input(rangeCol)))).getOrElse(df)
    }

    // Use the pipeline to cast columns to expected values and select them into a new dataframe
    val castCols = Map(xCol -> DoubleType.simpleString, yCol -> DoubleType.simpleString, rangeCol -> LongType.simpleString)

    val frame = Pipe(input)
      .to(convertTimes)
      .to(castColumns(castCols))
      .run()

    // Extracts lat, lon, time coordinates from row
    val coordExtractor = (r: Row) => {
      val xIndex = r.schema.fieldIndex(xCol)
      val yIndex = r.schema.fieldIndex(yCol)
      val rangeIndex = r.schema.fieldIndex(rangeCol)

      if (!r.isNullAt(xIndex) && !r.isNullAt(yIndex) && !r.isNullAt(rangeIndex)) {
        Some(r.getDouble(xIndex), r.getDouble(yIndex), r.getLong(rangeIndex))
      } else {
        None
      }
    }

    // create the series to tie everything together
    val series = new Series(
      (tileSize - 1, tileSize - 1, timeRange.count - 1),
      coordExtractor,
      projection,
      valueExtractor,
      binAggregator,
      tileAggregator
    )

    BasicSaltOperations.genericTiling(series)(request)(frame.rdd)
  }
}

