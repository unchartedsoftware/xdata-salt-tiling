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

import scala.collection.JavaConverters._ // scalastyle:off
import scala.util.Try

/**
  * Configuration specifying how basic XY heatmap tiling is to be performed.
  *
  * @param xColumn The dataframe column storing the X data
  * @param yColumn The dataframe column storing the Y data
  * @param valueColumn The dataframe column storing count values.  If unset, a count of one will be used.
  * @param projectionConfig The projection from data space to (tile, bin) space.
  */
case class XYHeatmapConfig(xCol: String,
                           yCol: String,
                           valueCol: Option[String],
                           projection: ProjectionConfig)

/**
  * Provides functions for parsing topic tile data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `xColumn` - The assigned name of the column containing the X values
  *   - `yColumn` - The assigned name of the column containing the Y values
  *   - `valueColumn` - The assigned name of column containing the a count value to use when creating the heatmap. If
  *                    unset, a count of 1 is associated with each row. [OPTIONAL]
  *   - `projection` - One of `cartesian` or `mercator`
  *   - `xyBounds` - Projection bounds as [minX, minY, maxX, maxY].  Points outside of these bounds will be
  *                  ignored.  This value is OPTIONAL for `mercator`, but required for `cartesian`.
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  xyHeatmap {
  *    xColumn = lon
  *    yColumn = lat
  *    valueColumn = text
  *    projection = cartesian
  *    xyBounds = [-84.0, 13.0, -50,0, 26.0]
  *  }
  *  }}}
  *
  */
object XYHeatmapConfig extends ConfigParser {
  private val rootKey = "xyHeatmap"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val valColumnKey = "valueColumn"

  /**
    * Parse general tiling parameters out of a config container and instantiates a `XYHeatmapConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `XYHeatmapConfig` object.
    */
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
