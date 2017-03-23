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
                              projection: ProjectionConfig)

object XYTimeTopicsConfig extends ConfigParser {
  private val xyTimeTopics = "xyTimeTopics"
  private val timeFormat = "timeFormat"
  private val xColumn = "xColumn"
  private val yColumn = "yColumn"
  private val timeColumn = "timeColumn"
  private val timeMin = "min"
  private val timeStep = "step"
  private val timeCount =  "count"
  private val textColumn = "textColumn"
  private val topicLimit = "topicLimit"
  private val termPath = "terms"

  def parse(config: Config): Try[XYTimeTopicsConfig] = {
    for (
      topicConfig <- Try(config.getConfig(xyTimeTopics));
      projection <- ProjectionConfig.parse(topicConfig)
    ) yield {
      XYTimeTopicsConfig(
        topicConfig.getString(xColumn),
        topicConfig.getString(yColumn),
        topicConfig.getString(timeColumn),
        topicConfig.getString(textColumn),
        RangeDescription.fromMin(topicConfig.getLong(timeMin), topicConfig.getLong(timeStep), topicConfig.getInt(timeCount)),
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
