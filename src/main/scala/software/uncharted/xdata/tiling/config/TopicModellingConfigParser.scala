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

import java.text.SimpleDateFormat

import scala.util.Try
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import software.uncharted.salt.xdata.util.RangeDescription
import software.uncharted.xdata.ops.topics.twitter.util.WordDict

case class TopicModellingConfig (
  alpha: Option[Double],
  beta: Option[Double],
  computeCoherence : Boolean,
  timeRange: RangeDescription[Long],
  dateCol : String,
  idCol : String,
  iterN: Option[Int],
  k: Option[Int],
  numTopTopics : Option[Int],
  pathToCorpus : String,
  pathToTfidf : String,
  stopwords: Set[String],
  textCol : String,
  pathToWrite: String
)

/**
  * Parse the config object into arguments for the topic modelling pipeline operations
  */
// scalastyle:off method.length
// scalastyle:off magic.number
object TopicModellingConfigParser extends ConfigParser with Logging{

  def parse(config: Config): Try[TopicModellingConfig] = {
    Try {
      val topicsConfig = config.getConfig("topics")

      //I think alpha should be optional.
      val alphaStr = topicsConfig.getString("alpha")
      val alpha = if (alphaStr == "1/Math.E") 1/Math.E else alphaStr.toDouble

      val startDate = topicsConfig.getString("startDate")
      val endDate = topicsConfig.getString("endDate")
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val minTime = formatter.parse(startDate).getTime
      val maxTime = formatter.parse(endDate).getTime
      val timeRange = RangeDescription.fromStep(minTime, maxTime, 24 * 60 * 60 * 1000).asInstanceOf[RangeDescription[Long]]

      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles").toArray[String](Array()).toList

      TopicModellingConfig(
        Some(alpha),
        getDoubleOption(topicsConfig, "beta"),
        topicsConfig.getBoolean("computeCoherence"),
        timeRange,
        topicsConfig.getString("dateColumn"),
        topicsConfig.getString("idColumn"),
        getIntOption(topicsConfig, "iterN"),
        getIntOption(topicsConfig, "k"),
        getIntOption(topicsConfig, "numTopTopics"),
        topicsConfig.getString("pathToCorpus"),
        getString(topicsConfig, "pathToTfidf", ""),
        WordDict.loadStopwords(swfiles),
        topicsConfig.getString("textColumn"),
        topicsConfig.getString("pathToWrite")
      )
    }
  }
}
