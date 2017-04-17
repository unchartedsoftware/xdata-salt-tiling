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

import scala.util.Try
import com.typesafe.config.Config

/**
  * A configuration object describing a file of character-separated values in HDFS
  *
  * @param location The HDFS location of the file.
  * @param partitions The number of partitions to read. If not specified, the default number of partitions will
  *                   be used.
  * @param separator A separator to use to separate columns in the CSV file.  Default is a comma.
  * @param neededColumns The columns from the CSV file that we need. Not optional.
  */
case class HdfsCsvConfig (location: String, partitions: Option[Int], separator: String, neededColumns: Seq[Int])

object HdfsCsvConfigParser extends ConfigParser {
  private val LOCATION_KEY = "location"
  private val PARTITIONS_KEY = "partitions"
  private val SEPARATOR_KEY = "separator"
  private val DEFAULT_SEPARATOR = ","
  private val RELEVANT_COLUMNS_KEY = "columns"
  /**
    * Read the config for a particular character-separated values file for input or output
    */
  def parse(key: String, defaultSeparator: String = DEFAULT_SEPARATOR)(config: Config): Try[HdfsCsvConfig] = {
    Try {
      val fileConfig = config.getConfig(key)
      HdfsCsvConfig(
        fileConfig.getString(LOCATION_KEY),
        getIntOption(fileConfig, PARTITIONS_KEY),
        getString(fileConfig, SEPARATOR_KEY, defaultSeparator),
        getIntList(fileConfig, RELEVANT_COLUMNS_KEY)
      )
    }
  }
}
