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
                          projection: ProjectionConfig)

object XYTopicsConfig extends ConfigParser {
  private val xyTopics = "xyTopics"
  private val xColumn = "xColumn"
  private val yColumn = "yColumn"
  private val valueColumn = "valueColumn"
  private val textColumn = "textColumn"
  private val topicLimit = "topicLimit"
  private val termPath = "terms"

  def parse(config: Config): Try[XYTopicsConfig] = {
    for (
      topicConfig <- Try(config.getConfig(xyTopics));
      projection <- ProjectionConfig.parse(topicConfig)
    ) yield {
      XYTopicsConfig(
        topicConfig.getString(xColumn),
        topicConfig.getString(yColumn),
        topicConfig.getString(valueColumn),
        topicConfig.getString(textColumn),
        topicConfig.getInt(topicLimit),
        readTerms(topicConfig.getString(termPath)),
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
