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

    val format = new SimpleDateFormat("yyyy-MM-dd") // if we add the ymd date cal first we dont need the formatter TODO can use other signature of this function
    val startDate = format.parse(startDateStr)
    val endDate = format.parse(endDateStr)

    // select
    val input2 = input.select(dateCol, idCol, textCol) // not necessary. unless we need a schema, in which case this simplifies things

    // filter date
    val input3 = dateFilter(startDate, endDate, "EEE MMM dd HH:mm:ss Z yyyy", "date")(input2) // if we hardcore date col name, we'll need a schema
    // TODO Alternative: rdd.filter(x => dates contains x(dateIndex))

    // add formatted date col (to beginning of Row)
    val datePsr = BTMUtil.makeTwitterDateParser()
    val input4 = addColumn("_ymd_date", (value: String) => {datePsr(value)}, dateCol)(input3)
    //    TODO does this get added in the 4th position? Should we swap it with 1st? to be used in the modelling? What did Craig pass in?

    // partition by date
    // FIXME TODO import joda-time
    // val numDays = org.joda.time.Days.daysBetween(new DateTime(startDate).toLocalDate(), new DateTime(endDate).toLocalDate()).getDays()
    val numDays = 3
    val input5 = input4.repartition(numDays, new Column("_ymd_date"))
    println(input5.groupBy("_ymd_date").count.show)
    println(input5.take(2).foreach(println))
    println(input5.printSchema)

    // run BTM on each partition
    // TODO do we have to collect?
    val parts = input5.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, textCol, tfidf_bcst)).collect

    parts.foreach(println)
    // Compute Coherence Scores for each of the topic distibutions
    // define number of top words to use to compute coherence score
    val topT = 10
    val cparts = TopicModellingUtil.castResults(parts)
    cparts.foreach(println)
    cparts.foreach { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
//      val topic_terms = topic_dist.map(x => x._2.toArray)
//      val textrdd = input.filter(x => x(0) == date).map(x => x(2))
      // takes a long time to calculate Coherence. Uncomment to enable // TODO make configurable
      // val (cs, avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, topT)
      // output_results(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir, cs.toArray, avg_cs)         // n.b. outputing coherence scores as well
      TopicModellingUtil.writeResultsToFile(topic_dist, nzMap, theta, phi, date, iterN, m, alpha, eta, duration, outdir)
    }
  }
}
