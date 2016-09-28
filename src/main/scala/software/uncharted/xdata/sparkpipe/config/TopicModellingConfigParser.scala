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

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import software.uncharted.xdata.ops.topics.util.{TFIDF, TopicModellingUtil, WordDict}

case class TopicModellingParams (
  alpha: Double,
  beta: Double,
  computeCoherence : Boolean,
  dateCol : String,
  endDate: String,
  idCol : String,
  iterN: Int,
  k: Int,
  numTopTopics : Int,
  outdir: String,
  path : String,
  startDate: String,
  stopwords_bcst: Broadcast[Set[String]],
  textCol : String,
  tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]]
)

/**
  * Parse the config object into arguments for the topic modelling pipeline operations
  */
// scalastyle:off method.length
object TopicModellingConfigParser extends Logging {
  def parse(config: Config, sc: SparkContext): TopicModellingParams = {
    try {
      val topicsConfig = config.getConfig("topics")
      val alpha = 1 / Math.E // topicsConfig.getDouble("alpha") // Interpreted by ConfigFactory as String, not Double
      val beta = if (topicsConfig.hasPath("beta")) topicsConfig.getDouble("beta") else 0.01
      val computeCoherence = topicsConfig.getBoolean("computeCoherence")
      val dateCol = topicsConfig.getString("dateColumn")
      val endDate = topicsConfig.getString("endDate")
      val idCol = topicsConfig.getString("idColumn")
      val iterN = if (topicsConfig.hasPath("iterN")) topicsConfig.getInt("iterN") else 150
      val k = if (topicsConfig.hasPath("k")) topicsConfig.getInt("k") else 2
      val numTopTopics = topicsConfig.getInt("numTopTopics")
      val outdir = topicsConfig.getString("outdir")
      val path = topicsConfig.getString("path")
      val startDate = topicsConfig.getString("startDate")
      val textCol = topicsConfig.getString("textColumn")

      // LM INPUT DATA
      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles").toArray[String](Array()).toList
      val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
      val stopwords_bcst = sc.broadcast(stopwords)

      val tfidf_path = if (config.hasPath("tfidf_path")) config.getString("tfidf_path") else ""
      val tfidf_bcst = if (!tfidf_path.isEmpty) {
        val tfidf_array = TFIDF.loadTfidf(tfidf_path, TopicModellingUtil.dateRange(startDate, endDate))
        Some(sc.broadcast(tfidf_array))
      } else {
        None
      }

      TopicModellingParams(
        alpha,
        beta,
        computeCoherence,
        dateCol,
        endDate,
        idCol,
        iterN,
        k,
        numTopTopics,
        outdir,
        path,
        startDate,
        stopwords_bcst,
        textCol,
        tfidf_bcst)

    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from Topic Modelling configuration file", e)
        sys.exit(-1) // FIXME Move someplace else?
    }
  }
}
