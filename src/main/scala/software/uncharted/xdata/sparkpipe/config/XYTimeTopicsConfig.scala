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

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import org.apache.commons.csv.CSVFormat
import software.uncharted.xdata.ops.salt.RangeDescription
import scala.collection.JavaConverters._ // scalastyle:ignore



// Parse config for mercator time heatmap sparkpipe op
case class XYTimeTopicsConfig(xCol: String,
                              yCol: String,
                              timeCol: String,
                              textCol: String,
                              timeRange: RangeDescription[Long],
                              timeFormat: Option[String],
                              topicLimit: Int,
                              termList: Map[String, String],
                              projection: Option[String] = None)

object XYTimeTopicsConfig extends Logging {

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

  def apply(config: Config): Option[XYTimeTopicsConfig] = {
    try {
      val topicConfig = config.getConfig(xyTimeTopicsKey)

      Some(XYTimeTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(timeColumnKey),
        topicConfig.getString(textColumnKey),
        RangeDescription.fromMin(topicConfig.getLong(timeMinKey), topicConfig.getLong(timeStepKey), topicConfig.getInt(timeCountKey)),
        if (topicConfig.hasPath(timeFormatKey)) Some(topicConfig.getString(timeFormatKey)) else None,
        topicConfig.getInt(topicLimitKey),
        readTerms(topicConfig.getString(termPathKey)),
        if (topicConfig.hasPath(projectionKey)) Some(topicConfig.getString(projectionKey)) else None)
      )
    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from [$xyTimeTopicsKey]", e)
        None
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
