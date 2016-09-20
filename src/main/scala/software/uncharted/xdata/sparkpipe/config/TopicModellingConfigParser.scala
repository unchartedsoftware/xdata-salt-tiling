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
import software.uncharted.xdata.ops.topics.{TFIDF, WordDict}
import software.uncharted.xdata.sparkpipe.jobs.util.TopicModellingJobUtil

case class TopicModellingParams (
  rdd: org.apache.spark.rdd.RDD[Array[String]],
  dates: Array[String],
  stopwords_bcst: Broadcast[Set[String]],
  iterN: Int,
  k: Int,
  alpha: Double,
  eta: Double ,
  outdir: String,
  weighted: Boolean,
  tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]]
)

object TopicModellingConfigParser extends Logging {
  def parse(config: Config, sc: SparkContext): TopicModellingParams = {
    try {
      // Load Data
      val loadConfig = config.getConfig("load") // XXX Split into two config option? one for loadTweets, one fo loadDates?
      val hdfspath = loadConfig.getString("hdfspath")
      val dates = loadConfig.getStringList("dates").asInstanceOf[Array[String]] // FIXME avoid cast. typesafe have a fix?
      val caIdx = loadConfig.getInt("createdAtIndex")
      val idIdx = loadConfig.getInt("twitterIdIndex")
      val textIdx = loadConfig.getInt("textIndex")
      val rdd = TopicModellingJobUtil.loadTweets(sc, hdfspath, dates, caIdx, idIdx, textIdx)

      val topicsConfig = config.getConfig("topics")
      val lang = topicsConfig.getString("lang")
      val iterN = topicsConfig.getInt("iterN")
      val alpha = topicsConfig.getDouble("alpha")
      val eta = topicsConfig.getDouble("eta")
      val k = topicsConfig.getInt("k")
      val outdir = topicsConfig.getString("outdir")

      // LM INPUT DATA
      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles").asInstanceOf[List[String]]
      val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
      val stopwords_bcst = sc.broadcast(stopwords)

      val tfidf_path = if (config.hasPath("tfidf_path")) config.getString("tfidf_path") else ""
      val weighted = if (tfidf_path != "") true else false
      val tfidf_bcst = if (weighted) {
        val tfidf_array = TFIDF.loadTfidf(tfidf_path, dates)
        Some(sc.broadcast(tfidf_array))
      } else { None }

      TopicModellingParams(rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir, weighted, tfidf_bcst)

    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from Topic Modelling configuration file", e)
        sys.exit(-1) // FIXME Move someplace else?
    }
  }
}
