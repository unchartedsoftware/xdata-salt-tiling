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

import java.text.SimpleDateFormat

import scala.util.Try
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import software.uncharted.xdata.ops.salt.RangeDescription
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
object TopicModellingConfigParser extends Logging {
  def apply(config: Config): Try[TopicModellingConfig] = {
    Try {
      val topicsConfig = config.getConfig("topics")
      //I think alpha should be optional.
      val alphaStr = topicsConfig.getString("alpha")
      val alpha = if (alphaStr == "1/Math.E") 1/Math.E else alphaStr.toDouble
      val beta = if (topicsConfig.hasPath("beta")) Some(topicsConfig.getDouble("beta")) else None
      val computeCoherence = topicsConfig.getBoolean("computeCoherence")
      val dateCol = topicsConfig.getString("dateColumn")
      val endDate = topicsConfig.getString("endDate")
      val idCol = topicsConfig.getString("idColumn")
      val iterN = if (topicsConfig.hasPath("iterN")) Some(topicsConfig.getInt("iterN")) else None
      val k = if (topicsConfig.hasPath("k")) Some(topicsConfig.getInt("k")) else None
      val numTopTopics = Some(topicsConfig.getInt("numTopTopics"))
      val pathToCorpus = topicsConfig.getString("pathToCorpus")
      val pathToTfidf = if (topicsConfig.hasPath("pathToTfidf")) topicsConfig.getString("pathToTfidf") else ""
      val startDate = topicsConfig.getString("startDate")
      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles").toArray[String](Array()).toList
      val stopwords = WordDict.loadStopwords(swfiles)
      val textCol = topicsConfig.getString("textColumn")
      val pathToWrite = topicsConfig.getString("pathToWrite")


      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val minTime = formatter.parse(startDate).getTime
      val maxTime = formatter.parse(endDate).getTime
      val timeRange = RangeDescription.fromStep(minTime, maxTime, 24 * 60 * 60 * 1000).asInstanceOf[RangeDescription[Long]]

      TopicModellingConfig(
        Some(alpha),
        beta,
        computeCoherence,
        timeRange,
        dateCol,
        idCol,
        iterN,
        k,
        numTopTopics,
        pathToCorpus,
        pathToTfidf,
        stopwords,
        textCol,
        pathToWrite
      )
    }
  }
}
