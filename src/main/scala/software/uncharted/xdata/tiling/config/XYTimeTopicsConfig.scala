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

import java.io.FileReader

import com.typesafe.config.Config
import org.apache.commons.csv.CSVFormat
import software.uncharted.salt.xdata.util.RangeDescription
import scala.collection.JavaConverters._ //scalastyle:ignore
import scala.util.Try

// Parse config for mercator time heatmap sparkpipe op
case class XYTimeTopicsConfig(xCol: String,
                              yCol: String,
                              timeCol: String,
                              textCol: String,
                              timeRange: RangeDescription[Long],
                              topicLimit: Int,
                              termList: Map[String, String],
                              projection: ProjectionConfig)

object XYTimeTopicsConfig extends ConfigParser{
  private val xyTimeTopicsKey = "xyTimeTopics"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val timeColumnKey = "timeColumn"
  private val timeMinKey = "min"
  private val timeStepKey = "step"
  private val timeCountKey =  "count"
  private val textColumnKey = "textColumn"
  private val topicLimitKey = "topicLimit"
  private val termPathKey = "terms"

  def parse(config: Config): Try[XYTimeTopicsConfig] = {
    for (
      topicConfig <- Try(config.getConfig(xyTimeTopicsKey));
      projection <- ProjectionConfig.parse(topicConfig)
    ) yield {
      XYTimeTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(timeColumnKey),
        topicConfig.getString(textColumnKey),
        RangeDescription.fromMin(topicConfig.getLong(timeMinKey), topicConfig.getLong(timeStepKey), topicConfig.getInt(timeCountKey)),
        topicConfig.getInt(topicLimitKey),
        readTerms(topicConfig.getString(termPathKey)),
        projection
      )
    }
  }

  private def readTerms(path: String) = {
    val in = new FileReader(path)
    val records = CSVFormat.DEFAULT
      .withAllowMissingColumnNames()
      .withCommentMarker('#')
      .withIgnoreSurroundingSpaces()
      .parse(in)
    records.iterator().asScala.map(x => (x.get(0), x.get(1))).toMap
  }
}
