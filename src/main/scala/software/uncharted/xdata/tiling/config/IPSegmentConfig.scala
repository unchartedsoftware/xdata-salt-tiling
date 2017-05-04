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
 * @param arcType        The type of line projection specified by the ArcType enum
 * @param minSegLen      The minimum length of line (in bins) to project
 * @param maxSegLen      The maximum length of line (in bins) to project
 * @param ipFromCol      start IP address
 * @param ipToCol        end IP address
 * @param valueCol       The name of the column which holds the value for a given row
 */
case class IPSegmentConfig(arcType: ArcTypes.Value,
                           minSegLen: Option[Int] = None,
                           maxSegLen: Option[Int] = None,
                           ipFromCol: String,
                           ipToCol: String,
                           valueCol: Option[String],
                           projectionConfig: ProjectionConfig)

/**
  * Provides functions for parsing IP segement heatmap data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `arcType` - The type of arc drawn.  Valid values are `fullline`, `leaderline`, `fullarc`, `leaderarc`
  *   - `minSegLen` - The minimum length of arc (in bins) to include.
  *   - `maxSegLen` - The maximum length of arc (in bins) to include.
  *   - `ipFromCol` - Name assigned to originating IPv4 or IPv6 address column.
  *   - `ipToCol` - Name assigned to destination IPv4 or IPv6 address column.
  *   - `valueColumn` - The assigned name of column containing the a count value to use when creating the heatmap. If none
  *                     if unset, a count of 1 is associated with each row. [OPTIONAL]
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  ipSegment {
  *    ipFromColumn = ip_v4_to_addr
  *    ipToColumn = ip_v4_to_addr
  *    arcType = fullline
  *  }
  *  }}}
  *
  */
object IPSegmentConfig extends ConfigParser {
  val RootKey = "ipSegment"
  private val ArcTypeKey = "arcType"
  private val MinSegLenKey = "minSegLen"
  private val MaxSegLenKey = "maxSegLen"
  private val IpFromCol = "ipFromColumn"
  private val IpToCol = "ipToColumn"
  private val ValueColumnKey = "valueColumn"

  /**
    * Parse general tiling parameters out of a config container and instantiates a `IPSegmentConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `IPSegmentConfig` object.
    */
  def parse(config: Config): Try[IPSegmentConfig] = {
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
      IPSegmentConfig(
        arcType,
        if (segmentConfig.hasPath(MinSegLenKey)) Some(segmentConfig.getInt(MinSegLenKey)) else None,
        if (segmentConfig.hasPath(MaxSegLenKey)) Some(segmentConfig.getInt(MaxSegLenKey)) else None,
        segmentConfig.getString(IpFromCol),
        segmentConfig.getString(IpToCol),
        getStringOption(segmentConfig, ValueColumnKey),
        projection
      )
    }
  }
}
