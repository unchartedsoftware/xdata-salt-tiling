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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.{Column, DataFrame}
import org.joda.time.{Days, DateTime}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.addColumn
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.dateFilter
import software.uncharted.xdata.ops.topics.twitter.util.{BDPParallel, BTMUtil, TopicModellingUtil, Coherence}

/**
  *
  */
package object twitter {
  // scalastyle:off parameter.number method.length
  def doTopicModelling(
    alpha: Double,
    beta: Double,
    computeCoherence: Boolean,
    dateCol: String,
    endDateStr: String,
    idCol: String,
    iterN: Int,
    k: Int,
    numTopTopics: Int,
    outdir: String,
    path: String,
    sqlContext: SQLContext,
    startDateStr: String,
    stopwords_bcst: Broadcast[Set[String]],
    textCol: String,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
  )(
    input : DataFrame
  ) : DataFrame = {

    val datePsr = BTMUtil.makeTwitterDateParser()
    var numPartitions : Int = Days.daysBetween(new DateTime(startDateStr).toLocalDate(), new DateTime(endDateStr).toLocalDate()).getDays()
    if (numPartitions.equals(2)) numPartitions += 1 // For some reason one partition is empty when partitioning a date range of length 2. Add a 3rd

    val data = Pipe(input)
      // select the columns we care about
      .to(_.select(dateCol, idCol, textCol))
      // Add formatted date col
      .to(addColumn("_ymd_date", (value: String) => {datePsr(value)}, dateCol))
      // filter tweets outside of date range
      .to(dateFilter(startDateStr, endDateStr, "yyyy-MM-dd", "_ymd_date"))
      // partition by date
      .to(_.repartition(numPartitions, new Column("_ymd_date")))
      .run

    // Run BTM on each partition
    val parts = data.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, beta, textCol, tfidf_bcst))
      .collect
      .filter(p => p.isDefined)
      .map(p => p.get)

    val cparts = TopicModellingUtil.castResults(parts)

    var coherenceMap : Option[scala.collection.mutable.Map[String,(Seq[Double],Double)]] = None
    if (computeCoherence) {
      coherenceMap = Some(Coherence.computeCoherence(cparts, input, numTopTopics, textCol))
    }

    TopicModellingUtil.writeTopicsToDF(
      alpha,
      beta,
      coherenceMap,
      computeCoherence,
      cparts,
      data,
      iterN,
      numTopTopics,
      outdir,
      sqlContext,
      textCol
    )
  }
}
