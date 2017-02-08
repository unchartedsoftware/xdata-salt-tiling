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
import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.util.Try

// Parse config for mercator time heatmap sparkpipe op
case class XYTopicsConfig(xCol: String,
                          yCol: String,
                          valueCol: String,
                          textCol: String,
                          topicLimit: Int,
                          termList: Map[String, String],
                          projection: Option[String] = None,
                          xyBounds: Option[(Double, Double, Double, Double)] = None)

object XYTopicsConfig {

  val xyTopicsKey = "xyTopics"
  val xColumnKey = "xColumn"
  val yColumnKey = "yColumn"
  val projectionKey = "projection"
  val valueColumnKey = "valueColumn"
  val textColumnKey = "textColumn"
  val topicLimitKey = "topicLimit"
  val termPathKey = "terms"
  val xyBoundsKey = "xyBounds"

  def apply(config: Config): Try[XYTopicsConfig] = {
    Try {
      val topicConfig = config.getConfig(xyTopicsKey)

      XYTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(valueColumnKey),
        topicConfig.getString(textColumnKey),
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
