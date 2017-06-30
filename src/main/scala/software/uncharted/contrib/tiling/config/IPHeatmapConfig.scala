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

package software.uncharted.contrib.tiling.config

import com.typesafe.config.Config

import scala.util.Try

/**
  * A config object representing information specific to IP tiling jobs
  *
  * @param ipCol The column in which an IP address is to be found
  * @param valueCol Optional column container containing a count value to be used.  If unset,
  *                 count value defaults to 1.
  */
case class IPHeatmapConfig (ipCol: String, valueCol: Option[String])

/**
  * Provides functions for parsing IP heatmap data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `ipColumn` - The assigned name of the column containing the IPv4 or IPv6 address string.
  *   - `valueColumn` - The assigned name of column containing the a count value to use when creating the heatmap. If none
  *                     if unset, a count of 1 is associated with each row. [OPTIONAL]
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  ipHeatmap {
  *    ipColumn = ip_v4_addr
  *    valueColumn = ip_v4_counts
  *  }
  *  }}}
  *
  */
object IPHeatmapConfig extends ConfigParser {
  val RootKey = "ipHeatmap"
  private val IpColumnKey = "ipColumn"
  private val ValueColumnKey = "valueColumn"

  /**
    * Parse general tiling parameters out of a config container and instantiates a `IPHeatmapConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `IPHeatmapConfig` object.
    */
  def parse (config: Config): Try[IPHeatmapConfig] = {
    Try{
      val ipConfig = config.getConfig(RootKey)
      IPHeatmapConfig(
        ipConfig.getString(IpColumnKey),
        getStringOption(ipConfig, ValueColumnKey)
      )
    }
  }
}
