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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileRequest
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.core.projection.Projection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.castColumns

// scalastyle:off parameter.number

/**
  * An operation which generates tile layers of point locations, located by IP address, from a DataFrame, using Salt
  */
object IPHeatmapOp {
  val defaultTileSize = 256
  val STRING_TYPE = "string"
  def apply[T, U, V, W, X](projection: Projection[String, (Int, Int, Int), (Int, Int)],
                           tileSize: Int,
                           ipCol: String,
                           vCol: String,
                           binAggregator: Aggregator[T, U, V],
                           tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val selectCols = Seq(ipCol, vCol).map(new Column(_))

    // Use the pipeline to convert x/y cols to doubles, and select them along with v col first
    val frame = Pipe(input)
      .to(castColumns(Map(ipCol -> STRING_TYPE)))
      .to(_.select(selectCols: _*))
      .run()

    val cExtractor = (r: Row) => {
      val ipIndex = r.schema.fieldIndex(ipCol)
      if (!r.isNullAt(ipIndex)) {
        Some(r.getString(ipIndex))
      } else {
        None
      }
    }

    val vExtractor = (r: Row) => {
      val rowIndex = r.schema.fieldIndex(vCol)
      if (!r.isNullAt(rowIndex)) {
        Some(r.getAs[T](rowIndex))
      } else {
        None
      }
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      binAggregator,
      tileAggregator
    )

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}

/**
  * An operation which generates tile layers of line segments, located from pairs of IP addresses, from a DataFrame,
  * using Salt
  */
object IPSegmentOp {
  val STRING_TYPE = "string"
  def apply[T, U, V, W, X]( // scalastyle:ignore
                            projection: Projection[(String, String), (Int, Int, Int), (Int, Int)],
                            tileSize: Int,
                            ipFromCol: String,
                            ipToCol: String,
                            vCol: String,
                            binAggregator: Aggregator[T, U, V],
                            tileAggregator: Option[Aggregator[V, W, X]]
                          )(request: TileRequest[(Int, Int, Int)])(input: DataFrame): RDD[SeriesData[(Int, Int, Int), (Int, Int), V, X]] = {

    val selectCols = Seq(ipFromCol, ipToCol, vCol).map(new Column(_))

    // Use the pipeline to convert x/y cols to doubles, and select them along with v col first
    val frame = Pipe(input)
      .to(castColumns(Map(ipFromCol -> STRING_TYPE, ipToCol -> STRING_TYPE)))
      .to(_.select(selectCols: _*))
      .run()

    val cExtractor = (r: Row) => {
      val fromIPIndex = r.schema.fieldIndex(ipFromCol)
      val toIPIndex = r.schema.fieldIndex(ipToCol)

      if (!r.isNullAt(fromIPIndex) && !r.isNullAt(toIPIndex)) {
        Some((r.getString(fromIPIndex), r.getString(toIPIndex)))
      } else {
        None
      }
    }

    val vExtractor = (r: Row) => {
      val rowIndex = r.schema.fieldIndex(vCol)
      if (!r.isNullAt(rowIndex)) {
        Some(r.getAs[T](rowIndex))
      } else {
        None
      }
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      binAggregator,
      tileAggregator
    )

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}
