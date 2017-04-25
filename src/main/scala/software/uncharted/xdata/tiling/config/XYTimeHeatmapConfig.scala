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
import software.uncharted.sparkpipe.ops.xdata.text.util.RangeDescription

import scala.util.Try

// Parse config for geoheatmap sparkpipe op
case class XYTimeHeatmapConfig(xCol: String,
                               yCol: String,
                               timeCol: String,
                               valueCol: Option[String],
                               timeRange: RangeDescription[Long],
                               projection: ProjectionConfig)
object XYTimeHeatmapConfig extends ConfigParser {

  val rootKey = "xyTimeHeatmap"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val timeColumnKey = "timeColumn"
  private val timeMinKey = "min"
  private val timeStepKey = "step"
  private val timeCountKey =  "count"
  private val valueColumnKey = "valueColumn"

  def parse(config: Config): Try[XYTimeHeatmapConfig] = {
    for (
      heatmapConfig <- Try(config.getConfig(rootKey));
      projection <- ProjectionConfig.parse(heatmapConfig)
    ) yield {
      XYTimeHeatmapConfig(
        heatmapConfig.getString(xColumnKey),
        heatmapConfig.getString(yColumnKey),
        heatmapConfig.getString(timeColumnKey),
        getStringOption(heatmapConfig, valueColumnKey),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
        projection
      )
    }
  }
}
