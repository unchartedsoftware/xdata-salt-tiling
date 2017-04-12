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
package software.uncharted.xdata.tiling.config

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
                          projection: ProjectionConfig)

object XYTopicsConfig extends ConfigParser {
  private val xyTopicsKey = "xyTopics"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val valueColumnKey = "valueColumn"
  private val textColumnKey = "textColumn"
  private val topicLimitKey = "topicLimit"
  private val termPathKey = "terms"

  def parse(config: Config): Try[XYTopicsConfig] = {
    for (
      topicConfig <- Try(config.getConfig(xyTopicsKey));
      projection <- ProjectionConfig.parse(topicConfig)
    ) yield {
      XYTopicsConfig(
        topicConfig.getString(xColumnKey),
        topicConfig.getString(yColumnKey),
        topicConfig.getString(valueColumnKey),
        topicConfig.getString(textColumnKey),
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