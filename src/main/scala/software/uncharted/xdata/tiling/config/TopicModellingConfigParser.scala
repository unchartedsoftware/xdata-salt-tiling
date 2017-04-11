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
