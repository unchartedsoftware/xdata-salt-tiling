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
import org.apache.spark.broadcast.Broadcast
import software.uncharted.xdata.sparkpipe.jobs.util.TopicModellingUtil
import org.apache.spark.rdd.RDD
import software.uncharted.xdata.ops.topics.{BDPParallel, DatePartitioner}
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TopicModellingConfigParser, TopicModellingParams}

// scalastyle:off method.length parameter.number
object TopicModellingParallelJob extends Logging {

  /*
  val path = "/xdata/data/twitter/isil-keywords/2016-09/isil_keywords.2016090 "
  val dates = Array("2016-09-03", "2016-09-04", "2016-09-05")
  val bdp = RunBDPParallel(sc)
  val rdd = bdp.loadTSVTweets(path, dates, 1, 0, 6)
   */

  //  TODO remove run alltogether? just have main. but nice to have param list here foor documentation
  def run(
           rdd: RDD[Array[String]],
           dates: Array[String],
           stopwords_bcst: Broadcast[Set[String]],
           iterN: Int,
           k: Int,
           alpha: Double,
           eta: Double,
           outdir: String,
           weighted: Boolean = false,
           tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
         ) : Unit = {
    // group records by date
    val kvrdd = BDPParallel.keyvalueRDD(rdd)
    // partition data by date

    val partitions = kvrdd.partitionBy(new DatePartitioner(dates))
    // run BTM on each partition
    val parts = partitions.mapPartitions { iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, tfidf_bcst) }.collect

    // Compute Coherence Scores for each of the topic distibutions
    // define number of top words to use to compute coherence score
    val topT = 10
    val cparts = TopicModellingUtil.castResults(parts)
    cparts.foreach { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
      val topic_terms = topic_dist.map(x => x._2.toArray)
      val textrdd = rdd.filter(x => x(0) == date).map(x => x(2))
      // takes a long time to calculate Coherence. Uncomment to enable // TODO make configurable
      // val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
      // output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)         // n.b. outputing coherence scores as well
      TopicModellingUtil.outputResults(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir)
    }
  }

  /**
    * Entrypoint
    *
    * @param args Array of commandline arguments
    */
  def main(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length != 1) {
      logger.error("Usage: ") // TODO
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config: Config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader()).resolve()
    val sparkContext: SparkContext = SparkConfig(config).sparkContext
    val params: TopicModellingParams = TopicModellingConfigParser.parse(config, sparkContext)

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
