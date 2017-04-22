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

import java.io.{File, FileReader}
import java.nio.file.Files

import com.typesafe.config.Config
import software.uncharted.sparkpipe.ops.xdata.salt.TopicsOp

import scala.collection.JavaConverters._
import scala.util.Try

case class XYHeatmapConfig(xCol: String,
                           yCol: String,
                           valueCol: Option[String],
                           projection: ProjectionConfig)


/**
  * Code for parsing XY Topic configuration settings.  Parameters are:
  *
  * xColumn - name of the column containing X values in the input DataFrame
  *
  * yColumn - name of the column containing Y values in the input DataFrame
  *
  * valColumn - OPTIONAL the name of the column containing values to sum in the DataFrame.  If this
  *   is unset the
  *
  * projection - a string value of 'mercator' or 'cartesian' indicating the projection to apply
  *
  * xyBounds - a tuple of doubles indicating the min and max bounds (minX, minY, maxX, maxY).  Rows with X,Y
  *   values outside of this region will be filtered out of the data set prior to tiling.  Bounds are required
  *   for cartesian projections, but will be defaulted to (-180, -90, 180, 90) for mercator projections if not
  *   supplied
  */
object XYHeatmapConfig extends ConfigParser {
  private val rootKey = "xyHeatmap"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val valColumnKey = "valueColumn"

  def parse(config: Config): Try[XYHeatmapConfig] = {
    for (
      heatmapConfig <- Try(config.getConfig(rootKey));
      projection <- ProjectionConfig.parse(heatmapConfig)
    ) yield {
      XYHeatmapConfig(
        heatmapConfig.getString(xColumnKey),
        heatmapConfig.getString(yColumnKey),
        getStringOption(heatmapConfig, valColumnKey),
        projection
      )
    }
  }
}
