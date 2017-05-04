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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.{TileLevelRequest, TileRequest}
import software.uncharted.salt.core.generation.{Series, TileGenerator}

import scala.reflect.ClassTag

/**
  * Basic building-block operations from which other salt operations are built or composed.
  */
object BasicSaltOperations {
  def genericTiling[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X])
                                                             (request: TileRequest[TC])
                                                             (data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    val generator = TileGenerator(data.sparkContext)
    generator.generate(data, series, request).flatMap(t => series(t))
  }

  def genericFullTilingRequest[RT, DC, TC: ClassTag, BC, T, U, V, W, X] (series: Series[RT, DC, TC, BC, T, U, V, W, X],
                                                                         levels: Seq[Int],
                                                                         getZoomLevel: TC => Int)
                                                                        (data: RDD[RT]): RDD[SeriesData[TC, BC, V, X]] = {
    genericTiling(series)(new TileLevelRequest[TC](levels, getZoomLevel))(data)
  }

  /**
    * Get the bounds of specified columns in a dataframe
    *
    * @param columns The columns to examine
    * @param data The raw data to examine
    * @return The minimum and maximum of xCol, then the minimum and maximum of yCol.
    */
  def getBounds (columns: String*)(data: DataFrame): Seq[(Double, Double)] = {
    val selects = columns.flatMap(column => Seq(min(column), max(column)))
    val minMaxes = data.select(selects: _*).take(1)(0).toSeq.map(_ match {
      case d: Double => d
      case f: Float => f.toDouble
      case l: Long => l.toDouble
      case i: Int => i.toDouble
    })

    minMaxes.grouped(2).map(bounds => (bounds(0), bounds(1))).toSeq
  }
}
