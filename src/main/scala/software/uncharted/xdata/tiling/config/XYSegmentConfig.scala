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

/**
  * @param x1Col           Dataframe column containing x1 values
  * @param y1Col           Dataframe column containing y1 values
  * @param x2Col           Dataframe column containing x2 values
  * @param y2Col           Dataframe column containing y2 values
  * @param valueCol        The name of the column which holds the count value for a given row.  If unset defaults
  *                        to 1.
  * @param arcType         The type of line projection, specified by the ArcType enum
  * @param minSegLen       The minimum length of line (in bins) to project
  * @param maxSegLen       The maximum length of line (in bins) to project
  *
  */
case class XYSegmentConfig(arcType: ArcTypes.Value,
                           minSegLen: Option[Int] = None,
                           maxSegLen: Option[Int] = None,
                           x1Col: String,
                           y1Col: String,
                           x2Col: String,
                           y2Col: String,
                           valueCol: Option[String],
                           projectionConfig: ProjectionConfig)


/**
  * Provides functions for parsing segment heatmap data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `arcType` - The type of arc drawn.  Valid values are `fullline`, `leaderline`, `fullarc`, `leaderarc`
  *   - `minSegLen` - The minimum length of arc (in bins) to include. [OPTIONAL]
  *   - `maxSegLen` - The maximum length of arc (in bins) to include. [OPTIONAL]
  *   - `x1Column` - Name assigned to column containing first point's x coord
  *   - `y1Column` - Name assigned to column containing first point's y coord
  *   - `x2Column` - Name assigned to column containing second point's x coord
  *   - `y2Column` - Name assigned to column containing second point's y coord
  *   - `valueColumn` - The assigned name of column containing the a count value to use when creating the heatmap. If
  *                     if unset, a count of 1 is associated with each row. [OPTIONAL]
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  xySegment {
  *    x1Column = start_lon
  *    y1Column = start_lat
  *    x2Column = end_lon
  *    y2Column = end_lat
  *    minSegLen = 8
  *    maxSegLen = 2048
  *  }
  *  }}}
  *
  */
object XYSegmentConfig extends ConfigParser {
  val RootKey = "xySegment"
  private val ArcTypeKey = "arcType"
  private val MinSegLenKey = "minSegLen"
  private val MaxSegLenKey = "maxSegLen"
  private val X1ColKey = "x1Column"
  private val Y1ColKey = "y1Column"
  private val X2ColKey = "x2Column"
  private val Y2ColKey = "y2Column"
  private val ValueColumnKey = "valueColumn"

  /**
    * Parse general tiling parameters out of a config container and instantiates a `XYSegmentConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `XYSegmentConfig` object.
    */
  def parse(config: Config): Try[XYSegmentConfig] = {
    for (
      segmentConfig <- Try(config.getConfig(RootKey));
      projection <- ProjectionConfig.parse(segmentConfig)
    ) yield {
      val arcType: ArcTypes.Value = segmentConfig.getString(ArcTypeKey).toLowerCase match {
        case "fullline" => ArcTypes.FullLine
        case "leaderline" => ArcTypes.LeaderLine
        case "fullarc" => ArcTypes.FullArc
        case "leaderarc" => ArcTypes.LeaderArc
      }
      XYSegmentConfig(
        arcType,
        if (segmentConfig.hasPath(MinSegLenKey)) Some(segmentConfig.getInt(MinSegLenKey)) else None,
        if (segmentConfig.hasPath(MaxSegLenKey)) Some(segmentConfig.getInt(MaxSegLenKey)) else None,
        segmentConfig.getString(X1ColKey),
        segmentConfig.getString(Y1ColKey),
        segmentConfig.getString(X2ColKey),
        segmentConfig.getString(Y2ColKey),
        getStringOption(segmentConfig, ValueColumnKey),
        projection
      )
    }
  }
}
