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
import org.apache.spark.sql.{DataFrame, Row}
import software.uncharted.salt.core.analytic.numeric.{MinMaxAggregator, SumAggregator}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.generation.{Series, TileGenerator}
import software.uncharted.salt.xdata.projection.IPProjection
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe

/**
  * Factory for generating IP heatmap op functions.
  */
object IPHeatmapOp {
  val DefaultTileSize = 256
  val StringType = "string"


  /**
    * Uses Salt to generate heatmap tiles from an input Dataframe.  Inputs consist of
    * IPv4 or IPv6 addresses that will be mapped into a cartesian (tile,bin) coordinate
    * space using a Z space filling curve.
    *
    * @param ipCol The name of the dataframe column storing the IPv4 or IPv6 values.
    * @param valueCol The optional name of the column using the values to sum.  If no value
    *             is specified the value will default to 1 for each row.
    * @param zoomLevels The zoom levels to generate tile data for.
    * @param tileSize The size of the produced tiles in bins.
    * @param input A dataframe containing the data to tile.
    * @return The produced tiles as an RDD of SeriesData.
    */
  def apply(ipCol: String,
            valueCol: Option[String],
            zoomLevels: Seq[Int],
            tileSize: Int = DefaultTileSize)
           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]] = {

    val projection = new IPProjection(zoomLevels)

    val frame = Pipe(input).to(dataframe.castColumns(Map(ipCol -> StringType))).run()

    val cExtractor = (r: Row) => {
      val ipIndex = r.schema.fieldIndex(ipCol)
      if (!r.isNullAt(ipIndex)) {
        Some(r.getString(ipIndex))
      } else {
        None
      }
    }

    val vExtractor = (r: Row) => {
      valueCol.map { v =>
        val rowIndex = r.schema.fieldIndex(v)
        if (!r.isNullAt(rowIndex)) {
          Some(r.getDouble(rowIndex))
        } else {
          None
        }
      }.getOrElse(Some(1.0))
    }

    // create a series for our heatmap
    val series = new Series(
      (tileSize - 1, tileSize - 1),
      cExtractor,
      projection,
      vExtractor,
      SumAggregator,
      Some(MinMaxAggregator)
    )

    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)

    val sc = frame.sqlContext.sparkContext
    val generator = TileGenerator(sc)

    generator.generate(frame.rdd, series, request).flatMap(t => series(t))
  }
}
