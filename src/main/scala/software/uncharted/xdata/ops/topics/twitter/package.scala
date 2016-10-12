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
import software.uncharted.xdata.ops.topics.twitter.util.{BDPParallel, BTMUtil, TopicModellingUtil, Coherence, TFIDF}

/**
  *
  */
package object twitter {
  // scalastyle:off parameter.number method.length

  /**
    * Take a second DataFrame, representing precomputed tfidf scores, as input. Parse this DataFrame into a broadcast variable and use it to do topic modelling
    */
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
    pathToWrite: String,
    sqlContext: SQLContext,
    startDateStr: String,
    stopwords_bcst: Broadcast[Set[String]],
    textCol: String
  )(
    input: (DataFrame, DataFrame)
  ) : DataFrame = {

    // Read in the second dataframe as precomputed tfidf scores
    val tfidf_bcst = Some(sqlContext.sparkContext.broadcast(
      TFIDF.filterDateRange(
        TFIDF.loadTFIDF(input._2),
        TopicModellingUtil.dateRange(startDateStr, endDateStr)
      )
    ))

    doTopicModelling(
      alpha,
      beta,
      computeCoherence,
      dateCol,
      endDateStr,
      idCol,
      iterN,
      k,
      numTopTopics,
      pathToWrite,
      sqlContext,
      startDateStr,
      stopwords_bcst,
      textCol,
      tfidf_bcst
    )(input._1)
  }

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
    pathToWrite: String,
    sqlContext: SQLContext,
    startDateStr: String,
    stopwords_bcst: Broadcast[Set[String]],
    textCol: String,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
  )(
    input: DataFrame
  ) : DataFrame = {

    val datePsr = BTMUtil.makeTwitterDateParser()
    var numPartitions : Int = Days.daysBetween(new DateTime(startDateStr).toLocalDate(), new DateTime(endDateStr).toLocalDate()).getDays()
    if (numPartitions.equals(2)) numPartitions += 1 // For some reason one partition is empty when partitioning a date range of length 2. Add a 3rd

    val formatted_date_col = "_ymd_date"

    val data = Pipe(input) // select the corpus DataFrame
      // select the columns we care about
      .to(_.select(dateCol, idCol, textCol))
      // Add formatted date col
      .to(addColumn(formatted_date_col, (value: String) => {datePsr(value)}, dateCol))
      // filter tweets outside of date range
      .to(dateFilter(startDateStr, endDateStr, "yyyy-MM-dd", formatted_date_col)) // TODO "yyyy-MM-dd" configurable
      // partition by date
      .to(_.repartition(numPartitions, new Column(formatted_date_col)))
      .run

    // Run BTM on each partition
    val parts = data.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, beta, textCol, tfidf_bcst))
      .collect
      .filter(p => p.isDefined)
      .map(p => p.get)

    val cparts = TopicModellingUtil.castResults(parts)

    var coherenceMap : Option[scala.collection.mutable.Map[String,(Seq[Double],Double)]] = if (computeCoherence) Some(Coherence.computeCoherence(cparts, input, numTopTopics, textCol)) else None

    TopicModellingUtil.writeTopicsToDF(
      alpha,
      beta,
      coherenceMap,
      computeCoherence,
      cparts,
      data,
      iterN,
      numTopTopics,
      pathToWrite,
      sqlContext,
      textCol
    )
  }
}
