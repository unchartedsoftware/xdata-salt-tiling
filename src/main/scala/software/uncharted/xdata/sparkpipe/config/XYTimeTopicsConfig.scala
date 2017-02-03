/**
 * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted(tm), formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.xdata.sparkpipe.config

import java.io.FileReader

import com.typesafe.config.Config
import org.apache.commons.csv.CSVFormat
import software.uncharted.xdata.ops.salt.RangeDescription
import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.Try



// Parse config for mercator time heatmap sparkpipe op
case class XYTimeTopicsConfig(xCol: String,
                              yCol: String,
                              timeCol: String,
                              textCol: String,
                              timeRange: RangeDescription[Long],
                              topicLimit: Int,
                              termList: Map[String, String],
                              projection: Option[String] = None,
                              xyBounds: Option[(Double, Double, Double, Double)] = None)

object XYTimeTopicsConfig {

  val xyTimeTopicsKey = "xyTimeTopics"
  val timeFormatKey = "timeFormat"
  val xColumnKey = "xColumn"
  val yColumnKey = "yColumn"
  val projectionKey = "projection"
  val timeColumnKey = "timeColumn"
  val timeMinKey = "min"
  val timeStepKey = "step"
  val timeCountKey =  "count"
  val textColumnKey = "textColumn"
  val topicLimitKey = "topicLimit"
  val termPathKey = "terms"
  val xyBoundsKey = "xyBounds"

  def apply(config: Config): Try[XYTimeTopicsConfig] = {
    Try {
      val topicConfig = config.getConfig(xyTimeTopicsKey)

      XYTimeTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(timeColumnKey),
        topicConfig.getString(textColumnKey),
        RangeDescription.fromMin(topicConfig.getLong(timeMinKey), topicConfig.getLong(timeStepKey), topicConfig.getInt(timeCountKey)),
        topicConfig.getInt(topicLimitKey),
        readTerms(topicConfig.getString(termPathKey)),
        if (topicConfig.hasPath(projectionKey)) Some(topicConfig.getString(projectionKey)) else None,
        if (topicConfig.hasPath(xyBoundsKey)) {// scalastyle:ignore
          val xyBounds = topicConfig.getDoubleList(xyBoundsKey).toArray(Array(Double.box(0.0)))
          Some(xyBounds(0), xyBounds(1), xyBounds(2), xyBounds(3))
        } else None)
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
