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
package software.uncharted.xdata.ops.topics

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * TODO rename file package.scala?
  */
object TopicModelling{
  // scalastyle:off parameter.number
  // TODO combine learnTopics and learnTopicsParallel into one op and have parallel be a boolean parameter that you can turn on and off
  def learnTopicsParallel(
           dates: Array[String],
           stopwords_bcst: Broadcast[Set[String]],
           iterN: Int,
           k: Int,
           alpha: Double,
           eta: Double,
           outdir: String,
           weighted: Boolean = false,
           tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None,
           path: String,
           caIdx: Int,
           idIdx: Int,
           textIdx: Int
         )(sc : SparkContext) : Unit = {
    val input = TopicModellingUtil.loadTweets(sc, path, dates, caIdx, idIdx, textIdx) // TODO convert to proper op load(params)(sparkContext)
    // group records by date
    val kvrdd = BDPParallel.keyvalueRDD(input) // TODO split into seperate op?
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
      val textrdd = input.filter(x => x(0) == date).map(x => x(2))
      // takes a long time to calculate Coherence. Uncomment to enable // TODO make configurable
      // val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
      // output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)         // n.b. outputing coherence scores as well
      TopicModellingUtil.writeResultsToFile(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir)
    }
  }
}
