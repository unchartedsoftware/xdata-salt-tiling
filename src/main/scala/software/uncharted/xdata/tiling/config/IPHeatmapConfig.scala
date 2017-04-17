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

import scala.util.Try

/**
  * A config object representing information specific to IP tiling jobs
  * @param ipCol The column in which an IP address is to be found
  * @param valueCol The column in which the value to be used is found
  */
case class IPHeatmapConfig (ipCol: String, valueCol: String)
object IPHeatmapConfig extends ConfigParser {
  private val CATEGORY_KEY = "ipTiling"
  private val IP_COLUMN_KEY = "ipColumn"
  private val VALUE_COLUMN_KEY = "valueColumn"

  def parse (config: Config): Try[IPHeatmapConfig] = {
    Try{
      val ipConfig = config.getConfig(CATEGORY_KEY)

      IPHeatmapConfig(
        ipConfig.getString(IP_COLUMN_KEY),
        ipConfig.getString(VALUE_COLUMN_KEY)
      )
    }
  }
}
