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
case class TopicModellingParams (
  rdd: org.apache.spark.rdd.RDD,
  dates: List[String],
  stopwords_bcst: Broadcast[Set[String]],
  iterN: Int,
  k: Int,
  alpha: Double,
  eta Double: ,
  outdir: String,
  weighted: Boolean,
  tfidf_bcst: Broadcast[Array[(String, String, Double)]]
)

object TopicModellingConfigParser extends Logging {
  def parse(config: Config): Option[TopicModellingParams] = {
    try {
      // Load Data
      val loadConfig = config.getConfig("loadTSVTweets")
      val hdfspath = loadConfig.getString("hdfspath")
      val dates = loadConfig.getStringList("dates")
      val caIdx = loadConfig.getInt("createdAtIndex")
      val idIdx = loadConfig.getInt("twitterIdIndex")
      val textIdx = loadConfig.getInt("textIndex")
      val rdd = JobUtil.loadTSVTweets(hdfspath, dates, caIdx, idIdx, textIdx)

      val topicsConfig = config.getConfig("topics")
      val lang = topicsConfig.getString("lang")
      val iterN = topicsConfig.getInt("iterN")
      val alpha = topicsConfig.getDouble("alpha")
      val eta = topicsConfig.getDouble("eta")
      val k = topicsConfig.getInt("k")
      val outdir = topicsConfig.getString("outdir")

      // LM INPUT DATA
      val swfiles : List[String] = topicsConfig.getStringList("stopWordFiles")
      val stopwords = WordDict.loadStopwords(swfiles) ++ Set("#isis", "isis", "#isil", "isil")
      val stopwords_bcst = sc.broadcast(stopwords)

      val tfidf_path = if (config.hasPath("tfidf_path")) config.getString(tfidf_path_key)} else ""
      val weighted = if (tfidf_path != "") true else false
      val tfidf_bcst = if (weighted) {
        val tfidf_array = TFIDF.loadTfidf(tfidf_path, dates)
        sc.broadcast(tfidf_array)
      } else null // TODO None

      Some(TopicModellingParams(rdd, dates, stopwords_bcst, iterN, k, alpha, eta, outdir, weighted, tfidf_bcst)

    } catch {
      case e: ConfigException =>
        error(s"Failure parsing arguments from Topic Modelling configuration file", e)
        None
    }
  }
}
