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
  startDate: String,
  endDate: String,
  stopwords_bcst: Broadcast[Set[String]],
  iterN: Int,
  k: Int,
  alpha: Double,
  beta: Double,
  outdir: String,
  tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]],
  path : String, // TODO done? either break off into seperate or nested config. depend if read function is generic or topics specific
  dateCol : String,
  idCol : String,
  textCol : String,
  computeCoherence : Boolean,
  numTopTopics : Int
)

/**
  * Parse the config object into arguments for the topic modelling pipeline operations
  */
object TopicModellingConfigParser extends Logging {
  def parse(config: Config, sc: SparkContext): TopicModellingParams = {
    try {
      // Load Data
      val loadConfig = config.getConfig("load")
      val path = loadConfig.getString("path")
      val dateCol = loadConfig.getString("dateColumn")
      val idCol = loadConfig.getString("idColumn")
      val textCol = loadConfig.getString("textColumn")

      val topicsConfig = config.getConfig("topics")
      val startDate = topicsConfig.getString("startDate")
      val endDate = topicsConfig.getString("endDate")
      val iterN = if (topicsConfig.hasPath("iterN")) topicsConfig.getInt("iterN") else 150
      val alpha = 1 / Math.E // topicsConfig.getDouble("alpha") // Interpreted by ConfigFactory as String, not Double
      val beta = if (topicsConfig.hasPath("beta")) topicsConfig.getDouble("beta") else 0.01
      val k = if (topicsConfig.hasPath("k")) topicsConfig.getInt("k") else 2
      val outdir = topicsConfig.getString("outdir")
      val computeCoherence = topicsConfig.getBoolean("computeCoherence")
      val numTopTopics = topicsConfig.getInt("numTopTopics")

      // LM INPUT DATA
      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles").toArray[String](Array()).toList // FIXME avoid cast. typesafe have a fix?
      val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
      val stopwords_bcst = sc.broadcast(stopwords)

      val tfidf_path = if (config.hasPath("tfidf_path")) config.getString("tfidf_path") else ""
      val tfidf_bcst = if (!tfidf_path.isEmpty) {
        val tfidf_array = TFIDF.loadTfidf(tfidf_path, TopicModellingUtil.dateRange(startDate, endDate)) // TODO parse start-end date into array of dates
        Some(sc.broadcast(tfidf_array))
      } else {
        None
      }

      TopicModellingParams(
        startDate,
        endDate,
        stopwords_bcst,
        iterN,
        k,
        alpha,
        beta,
        outdir,
        tfidf_bcst,
        path,
        dateCol,
        idCol,
        textCol,
        computeCoherence,
        numTopTopics)

    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from Topic Modelling configuration file", e)
        sys.exit(-1) // FIXME Move someplace else?
    }
  }
}
