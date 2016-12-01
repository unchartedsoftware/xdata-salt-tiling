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
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import java.util.Date
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.dataframe.addColumn
import software.uncharted.sparkpipe.ops.core.dataframe.temporal.dateFilter
import software.uncharted.xdata.ops.salt.RangeDescription
import software.uncharted.xdata.ops.topics.twitter.util.{BDPParallel, BTMUtil, TFIDF, TopicModellingUtil}

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
    * @param dateCol The column of the input DataFrame in which to find the date
    * @param idCol The column of the input DataFrame in which to find the id
    * @param textCol The column of the input DataFrame in which to find the text
    * @param outputCol The column of the output DataFrame in which the result will be written.
    *                  It will contain a row with two columns: topic & probability.
    * @param sqlContext A SQLContext object used to create the resulting DataFrame
    * @param alpha A dirichlet hyperparameter of the clumpiness of the model. Defaults to 1/e
    * @param beta The value of beta. Defaults to 0.01
    * @param startDateStr Beginning (inclusive) of the date range you are running this job over
    * @param endDateStr The end (inclusive) of the date range this job is to be run over
    * @param iterN The number of iterations of MCMC sampling to run. Defaults to 150
    * @param k The number of topics to start with. Defaults to 2
    * @param numTopTopics Number of topics to output
    * @param stopwords_bcst All word to be ignored as potential topics
    *
    * @param input A tuple of DataFrames of the form: (corpus data, tfidf scores)
    */
  // scalastyle:off parameter.number method.length magic.number
  def getDocumentTopicRawTFIDF(
                        dateCol: String,
                        idCol: String,
                        textCol: String,
                        outputCol: String,
                        sqlContext: SQLContext,
                        alpha: Double = 1 / Math.E,
                        beta: Double = 0.01,
                        timeRange: RangeDescription[Long],
                        iterN: Int = 150,
                        k: Int = 2,
                        numTopTopics: Int = 1,
                        stopwords_bcst: Broadcast[Set[String]]
                      )(
                        input: (DataFrame, DataFrame)
                      ) : DataFrame = {

    // Read in the second dataframe as precomputed tfidf scores
    val tfidf_bcst = Some(sqlContext.sparkContext.broadcast(
      TFIDF.filterDateRange(
        TFIDF.loadTFIDF(input._2),
        TopicModellingUtil.dateRange(new Date(timeRange.min), new Date(timeRange.max))
      )
    ))

    getDocumentTopic(
      dateCol,
      idCol,
      textCol,
      outputCol,
      sqlContext,
      alpha,
      beta,
      timeRange,
      iterN,
      k,
      numTopTopics,
      stopwords_bcst,
      tfidf_bcst
    )(input._1)
  }

  /**
    * Preform topic modelling
    * @param dateCol The column of the input DataFrame in which to find the date
    * @param idCol The column of the input DataFrame in which to find the id
    * @param textCol The column of the input DataFrame in which to find the text
    * @param outputCol The column of the output DataFrame in which the result will be written.
    *                  It will contain a row with two columns: topic & probability.
    * @param sqlContext A SQLContext object used to create the resulting DataFrame
    * @param alpha A dirichlet hyperparameter of the clumpiness of the model. Defaults to 1/e
    * @param beta The value of beta. Defaults to 0.01
    * @param startDateStr Beginning (inclusive) of the date range you are running this job over
    * @param endDateStr The end (inclusive) of the date range this job is to be run over
    * @param iterN The number of iterations of MCMC sampling to run. Defaults to 150
    * @param k The number of topics to start with. Defaults to 2
    * @param numTopics Number of topics to output
    * @param stopwords_bcst All word to be ignored as potential topics
    * @param tfidf_bcst The precomputed tfidf scores
    *
    * @param input The corpus data
    */
  def getDocumentTopic(
                        dateCol: String,
                        idCol: String,
                        textCol: String,
                        outputCol: String,
                        sqlContext: SQLContext,
                        alpha: Double = 1 / Math.E,
                        beta: Double = 0.01,
                        timeRange: RangeDescription[Long],
                        iterN: Int = 150,
                        k: Int = 2,
                        numTopics: Int = 1,
                        stopwords_bcst: Broadcast[Set[String]],
                        tfidf_bcst: Option[Broadcast[Array[(String, String, Double)]]] = None
                      )(
                        input: DataFrame
                      ) : DataFrame = {

    // create a date parser specific to the input data
    val datePsr = BTMUtil.makeTwitterDateParser()
    // Choose the number of partitions. Namely, the number of days in the date range
    //var numPartitions : Int = Days.daysBetween(new DateTime(startDateStr).toLocalDate(), new DateTime(endDateStr).toLocalDate()).getDays()
    var numPartitions : Int = timeRange.count
    if (numPartitions.equals(2)) numPartitions += 1 // For some reason one partition is empty when partitioning a date range of length 2. Add a 3rd

    val formatted_date_col = "_ymd_date"

    // Prepare the data for BTM
    val data = Pipe(input)
      // select the columns we care about
      .to(_.select(dateCol, idCol, textCol))
      // Add formatted date col
      .to(addColumn(formatted_date_col, (value: String) => {datePsr(value)}, dateCol))
      // filter tweets outside of date range
      .to(dateFilter(new Date(timeRange.min), new Date(timeRange.max), "yyyy-MM-dd", formatted_date_col))
      // partition by date
      .to(_.repartition(numPartitions, new Column(formatted_date_col)))
      .run

    // Run BTM on each partition
    val tweetTopics = data.mapPartitions(iter => BDPParallel.partitionBDP(iter, stopwords_bcst, iterN, k, numTopics, alpha, beta, textCol, formatted_date_col, idCol, tfidf_bcst))
    sqlContext.createDataFrame(tweetTopics, BDPParallel.getTweetTopicSchema(data.schema, outputCol))
  }
}
