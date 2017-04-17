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
import software.uncharted.sparkpipe.ops.xdata.salt.ArcTypes

import scala.util.Try

// Parse config for segment sparkpipe op
case class XYSegmentConfig(arcType: ArcTypes.Value,
                           minSegLen: Option[Int] = None,
                           maxSegLen: Option[Int] = None,
                           x1Col: String,
                           y1Col: String,
                           x2Col: String,
                           y2Col: String,
                           valueCol: Option[String],
                           tileSize: Int,
                           projectionConfig: ProjectionConfig)

object XYSegmentConfig extends ConfigParser {
  private val xySegmentKey = "xySegment"
  private val arcTypeKey = "arcType"
  private val minSegLenKey = "minSegLen"
  private val maxSegLenKey = "maxSegLen"
  private val x1ColKey = "x1Column"
  private val y1ColKey = "y1Column"
  private val x2ColKey = "x2Column"
  private val y2ColKey = "y2Column"
  private val tileSizeKey = "tileSize"
  private val valueColumnKey = "valueColumn"

  def parse(config: Config): Try[XYSegmentConfig] = {
    for (
      segmentConfig <- Try(config.getConfig(xySegmentKey));
      projection <- ProjectionConfig.parse(segmentConfig)
    ) yield {
      val arcType: ArcTypes.Value = segmentConfig.getString(arcTypeKey).toLowerCase match {
        case "fullline" => ArcTypes.FullLine
        case "leaderline" => ArcTypes.LeaderLine
        case "fullarc" => ArcTypes.FullArc
        case "leaderarc" => ArcTypes.LeaderArc
      }
      XYSegmentConfig(
        arcType,
        if (segmentConfig.hasPath(minSegLenKey)) Some(segmentConfig.getInt(minSegLenKey)) else None,
        if (segmentConfig.hasPath(maxSegLenKey)) Some(segmentConfig.getInt(maxSegLenKey)) else None,
        segmentConfig.getString(x1ColKey),
        segmentConfig.getString(y1ColKey),
        segmentConfig.getString(x2ColKey),
        segmentConfig.getString(y2ColKey),
        getStringOption(segmentConfig, valueColumnKey),
        segmentConfig.getInt(tileSizeKey),
        projection
      )
    }
  }
}
