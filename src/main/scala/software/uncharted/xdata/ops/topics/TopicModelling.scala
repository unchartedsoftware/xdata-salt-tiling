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

import java.text.SimpleDateFormat
//import joda.time.Days
//import joda.time.DateTime
//import org.joda.time.DateTime
import org.apache.spark.SparkContext
import software.uncharted.sparkpipe.Pipe
import org.apache.spark.broadcast.Broadcast
import software.uncharted.sparkpipe.ops.core.dataframe.addColumn
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.dateFilter
import org.apache.spark.sql.{Column, DataFrame, Row}



/**
  * TODO rename file package.scala?
  */
object TopicModelling{
  // scalastyle:off parameter.number method.length
  // TODO combine learnTopics and learnTopicsParallel into one op and have parallel be a boolean parameter that you can turn on and off
  def learnTopicsParallel(
    startDateStr: String,
    endDateStr: String,
    stopwords_bcst: Broadcast[Set[String]],
    iterN: Int,
    k: Int,
    alpha: Double,
    eta: Double,
    outdir: String,
    weighted: Boolean = false,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None,
    path: String,
    dateCol: String, // TODO depricate in favour of column names
    idCol: String,
    textCol: String
  )(
    input : DataFrame
  ) : Unit = {

    // define number of top words to use to compute coherence score
    val topT = 10
    val format = new SimpleDateFormat("yyyy-MM-dd") // if we add the ymd date cal first we dont need the formatter TODO can use other signature of this function
    val datePsr = BTMUtil.makeTwitterDateParser()
    // FIXME TODO import joda-time
    // val numPartitions = org.joda.time.Days.daysBetween(new DateTime(startDate).toLocalDate(), new DateTime(endDate).toLocalDate()).getDays()
    val numPartitions = 2 + 1

    val data = Pipe(input)
      // select the columns we care about
      .to(_.select(dateCol, idCol, textCol))
      // filter tweets outside of date range
      .to(dateFilter(format.parse(startDateStr), format.parse(endDateStr), "EEE MMM dd HH:mm:ss Z yyyy", dateCol))
      // Add formatted date col (to beginning of Row)
      .to(addColumn("_ymd_date", (value: String) => {datePsr(value)}, dateCol))
      // partition by date XXX for some reason one partition is empty when partitioning two dates. Fixed by requiring 3 partitions (instead of 2)
      .to(_.repartition(numPartitions, new Column("_ymd_date")))
      .run

    // println(input5.groupBy("_ymd_date").count.show)

    // Run BTM on each partition
    val parts = data.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, textCol, tfidf_bcst))
      .collect
      .filter(p => p.isDefined)
      .map(p => p.get)
    // Compute Coherence Scores for each of the topic distibutions
    val cparts = TopicModellingUtil.castResults(parts)
    cparts.foreach { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
//      val topic_terms = topic_dist.map(x => x._2.toArray)
//      val textrdd = input.filter(x => x(0) == date).map(x => x(2))
      // takes a long time to calculate Coherence. Uncomment to enable // TODO make configurable
      // val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
      // output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)         // n.b. outputing coherence scores as well // API change cs and avgcs are now options TODO
      //  TODO write output to different format? as a dataframe?
      TopicModellingUtil.writeResultsToFile(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir)
    }
  }
}
