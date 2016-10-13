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
  * This package contains the operation (doTopicModelling) to compute the topics of a given corpus.
  * This operation can optionally perform tfidf processing. To do this, pass the operation a tuple
  * of DataFrames where the second represents your pre-computed tfidf scores
  */
package object twitter {

  /**
    * Perform Topic Modelling
    * Take a second DataFrame, representing precomputed tfidf scores, as input.
    * Parse this DataFrame into a broadcast variable and use it to do topic modelling
    *
    * @param alpha The value of alpha. Defaults to 1/e
    * @param beta The value of beta. Defaults to 0.01
    * @param computeCoherence  Whether or not to compute the coherence score of each topic
    * @param dateCol The column of the input DataFrame in which to find the date
    * @param endDateStr The end (inclusive) of the date range this job is to be run over
    * @param idCol The column of the input DataFrame in which to find the id
    * @param iterN The number of iterations. Defaults to 150
    * @param k The value of k. Defaults to 2
    * @param numTopTopics The number of top topics to output
    * @param pathToWrite The path to the data (on which to do the topic modelling)
    * @param sqlContext A SQLContext object used to create the resulting DataFrame
    * @param startDateStr Beginning (inclusive) of the date range you are running this job over
    * @param stopwords_bcst All word to be ignored as potential topics
    * @param textCol Column in which to find the text column
    *
    * @param input A tuple of DataFrames of the form: (corpus data, tfidf scores)
    */
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

  /**
    * Preform topic modelling
    * @param alpha The value of alpha. Defaults to 1/e
    * @param beta The value of beta. Defaults to 0.01
    * @param computeCoherence  Whether or not to compute the coherence score of each topic
    * @param dateCol The name of the column of the input DataFrame in which to find the date
    * @param endDateStr The end (inclusive) of the date range this job is to be run over
    * @param idCol The name of the column of the input DataFrame in which to find the id
    * @param iterN The number of iterations. Defaults to 150
    * @param k The value of k. Defaults to 2
    * @param numTopTopics The number of top topics to output
    * @param pathToWrite The path to the data (on which to do the topic modelling)
    * @param sqlContext A SQLContext object used to create the resulting DataFrame
    * @param startDateStr Beginning (inclusive) of the date range you are running this job over
    * @param stopwords_bcst All word to be ignored as potential topics
    * @param textCol The name of the column in which to find the text column
    * @param tfidf_bcst The precomputed tfidf scores
    *
    * @param input The corpus data
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
    textCol: String,
    tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
  )(
    input: DataFrame
  ) : DataFrame = {

    // create a date parser specific to the input data
    val datePsr = BTMUtil.makeTwitterDateParser()
    // Choose the number of partitions. Namely, the number of days in the date range
    var numPartitions : Int = Days.daysBetween(new DateTime(startDateStr).toLocalDate(), new DateTime(endDateStr).toLocalDate()).getDays()
    if (numPartitions.equals(2)) numPartitions += 1 // For some reason one partition is empty when partitioning a date range of length 2. Add a 3rd

    val formatted_date_col = "_ymd_date"

    // Prepare the data for BTM
    val data = Pipe(input)
      // select the columns we care about
      .to(_.select(dateCol, idCol, textCol))
      // Add formatted date col
      .to(addColumn(formatted_date_col, (value: String) => {datePsr(value)}, dateCol))
      // filter tweets outside of date range
      .to(dateFilter(startDateStr, endDateStr, "yyyy-MM-dd", formatted_date_col))
      // partition by date
      .to(_.repartition(numPartitions, new Column(formatted_date_col)))
      .run

    // Run BTM on each partition
    val parts = data.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, alpha, beta, textCol, tfidf_bcst))
      .collect
      .filter(p => p.isDefined)
      .map(p => p.get)

    // Cast from `Any` to the proper types
    val cparts = TopicModellingUtil.castResults(parts)

    // If requested, compute the coherence score for each result
    var coherenceMap : Option[Map[String,(Seq[Double],Double)]] = if (computeCoherence) Some(Coherence.computeCoherence(cparts, input, numTopTopics, textCol)) else None

    // Create a dataframe of the results
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
