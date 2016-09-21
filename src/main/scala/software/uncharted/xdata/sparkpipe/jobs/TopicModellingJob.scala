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
package software.uncharted.xdata.sparkpipe.jobs

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import software.uncharted.xdata.ops.topics.{BDP, BTMUtil, WordDict}
import software.uncharted.xdata.sparkpipe.jobs.util.TopicModellingUtil
import software.uncharted.xdata.ops.topics.Coherence
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TopicModellingConfigParser, TopicModellingParams}

// scalastyle:off method.length parameter.number
object TopicModellingJob extends Logging {

  def run(
    rdd: RDD[Array[String]],
    dates: Array[String],
    stopwords_bcst: Broadcast[Set[String]], // TODOO rename all broadcast variables to b<name>
    iterN: Int,
    k: Int,
    alpha: Double,
    eta: Double,
    outdir: String,
    weighted: Boolean = false,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
  ) : Unit = {

    val topT = 10

    dates.foreach{date =>
      val textrdd = rdd.filter(x => x(0) == date).map(x => x(2))
      val texts = textrdd.distinct.collect
      val stopwords = stopwords_bcst.value
      val minCount = 0
      val (word_dict, words) = WordDict.createWordDictLocal(texts, stopwords, minCount)
      val m = words.length
      val bdp = new BDP(k)
      val biterms = texts.map(text => BTMUtil.extractBitermsFromTextRandomK(text, word_dict, stopwords.toSet, k)).flatMap(x => x)

      if (weighted) bdp.initTfidf(tfidf_bcst.get, date, word_dict) // weighted redundant, use tfidf_bcst.getOrElse

      val (topic_dist, theta, phi, nzMap, duration) = bdp.fit(biterms, words, iterN, k, alpha, eta, weighted)
//      val result = List(Array(date, topic_dist, theta, phi, nzMap, m, duration)).iterator

      val topic_terms = topic_dist.map(x => x._2.toArray)
      val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
      TopicModellingUtil.outputResults(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)
    }
  }

  /**
    * Entrypoint
    *
    * @param args Array of commandline arguments
    */
  def main(args: Array[String]): Unit = { // TODO remove args?

    val config : Config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader()).resolve()
    val sparkContext : SparkContext = SparkConfig(config).sparkContext
    val params : TopicModellingParams = TopicModellingConfigParser.parse(config, sparkContext) // TODO remove sparkContext as param

    run(
      params.rdd,
      params.dates,
      params.stopwords_bcst,
      params.iterN,
      params.k,
      params.alpha,
      params.eta,
      params.outdir,
      params.weighted,
      params.tfidf_bcst
    )
    sparkContext.stop()
  }
}
