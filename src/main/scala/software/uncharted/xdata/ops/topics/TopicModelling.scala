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
    startDate: String,
    endDate: String,
    stopwords_bcst: Broadcast[Set[String]],
    iterN: Int,
    k: Int,
    alpha: Double,
    eta: Double,
    outdir: String,
    weighted: Boolean = false,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None,
    path: String,
    caIdx: Int, // TODO depricate in favour of column names
    idIdx: Int,
    textIdx: Int
  )(
    input : DataFrame
  ) : Unit = {

    val createdAtCol = "date"
    val idCol = "id"
    val textCol = "text"

    // select
    val input2 = input.select(createdAtCol, idCol, textCol) // not necessary. unless we need a schema, in which case this simplifies things

    // filter date
    val format = new SimpleDateFormat("yyyy-MM-dd") // if we add the ymd date cal first we dont need the formatter TODO can use other signature of this function
    val input3 = dateFilter(format.parse(startDate), format.parse(endDate), "EEE MMM dd HH:mm:ss Z yyyy", "date")(input2) // if we hardcore date col name, we'll need a schema
    // TODO Alternative: rdd.filter(x => dates contains x(caIdx))

    // add formatted date col
    val datePsr = BTMUtil.makeTwitterDateParser()
    val input4 = addColumn("_ymd_date", (value: String) => {datePsr(value)}, "date")(input3) // dont know date col name TODO
    //    TODO does this get added in the 4th position? Should we swap it with 1st? to be used in the modelling? What did Craig pass in?

    println(input4.count)
     println(input4.explain)
    // partition by date
    val input5 = input4.repartition(new Column("_ymd_date"))

    println(input5.count)
    println(input5.groupBy("_ymd_date").count.show)

     println(input5.explain)
    // input5.take(1).foreach(println)

    // group records by date
    // val kvrdd = BDPParallel.keyvalueRDD(input) // TODO split into seperate op?
    // val partitions = kvrdd.partitionBy(new DatePartitioner(dates))
    // run BTM on each partition
    // val parts = partitions.mapPartitions { iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, tfidf_bcst) }.collect
    // TODO do we have to collect?
    val parts = input4.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, eta, weighted, tfidf_bcst)).collect


    // Compute Coherence Scores for each of the topic distibutions
    // define number of top words to use to compute coherence score
    val topT = 10
    val cparts = TopicModellingUtil.castResults(parts)
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
