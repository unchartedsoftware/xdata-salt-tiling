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

import java.io.{File, FileReader}
import java.nio.file.Files

import com.typesafe.config.Config
import software.uncharted.sparkpipe.ops.xdata.text.util.RangeDescription

import scala.collection.JavaConverters._ //scalastyle:off
import scala.util.Try

// Parse config for mercator time heatmap sparkpipe op
case class XYTimeTopicsConfig(xCol: String,
                              yCol: String,
                              timeCol: String,
                              textCol: String,
                              timeRange: RangeDescription[Long],
                              topicLimit: Int,
                              termList: Seq[String],
                              stopWordsList: Seq[String],
                              projection: ProjectionConfig)

/**
  * Code for parsing XY Topic configuration settings.  Parameters are:
  *
  * xColumn - name of the column containing X values in the input DataFrame
  *
  * yColumn - name of the column containing Y values in the input DataFrame
  *
  * textColumn - the name of the column containing the text values in the input DataFrame
  *
  * topicLimit - the maximum number of topics to track per tile
  *
  * terms - a file or resource path pointing to a list of terms of interest.  Rows that do not contain at
  *   least one of the terms from the list will be filtered out of the data set prior to tiling.
  *
  * stopwords - a file or resource path point to a list of words to ignore when computing term frequencies
  *   for a tile
  *
  * projection - a string value of 'mercator' or 'cartesian' indicating the projection to apply
  *
  * xyBounds - a tuple of doubles indicating the min and max bounds (minX, minY, maxX, maxY).  Rows with X,Y
  *   values outside of this region will be filtered out of the data set prior to tiling.  Bounds are required
  *   for cartesian projections, but will be defaulted to (-180, -90, 180, 90) for mercator projections if not
  *   supplied
  */
object XYTimeTopicsConfig extends ConfigParser{
  private val rootKey = "xyTimeTopics"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val timeColumnKey = "timeColumn"
  private val timeMinKey = "min"
  private val timeStepKey = "step"
  private val timeCountKey =  "count"
  private val textColumnKey = "textColumn"
  private val topicLimitKey = "topicLimit"
  private val termPathKey = "terms"
  private val stopWordPathKey = "stopWords"

  def parse(config: Config): Try[XYTimeTopicsConfig] = {
    for (
      topicConfig <- Try(config.getConfig(rootKey));
      projection <- ProjectionConfig.parse(topicConfig)
    ) yield {
      XYTimeTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(timeColumnKey),
        topicConfig.getString(textColumnKey),
        RangeDescription.fromMin(topicConfig.getLong(timeMinKey),
                                 topicConfig.getLong(timeStepKey),
                                 topicConfig.getInt(timeCountKey)),
        topicConfig.getInt(topicLimitKey),
        getStringOption(topicConfig, termPathKey).map(readTerms(_)).getOrElse(Seq()),
        getStringOption(topicConfig, stopWordPathKey).map(readTerms(_)).getOrElse(Seq()),
        projection
      )
    }
  }

  private def readTerms(path: String) = {
    val file = new File(path)
    if (file.exists()) {
      Files.readAllLines(file.toPath).asScala
    } else {
      val stream = getClass.getResourceAsStream(path)
      scala.io.Source.fromInputStream(stream).getLines.toSeq
    }
  }
}
