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
import org.apache.spark.sql.{SQLContext, DataFrame}
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.topics.twitter.doTopicModelling
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TopicModellingConfigParser, TopicModellingParams}

// scalastyle:off method.length parameter.number
object TopicModellingJob extends Logging {

  /**
    * This job runs the topic modelling op
    *
    * This job takes a config file as its only argument. The config file contains all parameters needed to run this job.
    * The config file is of the format:
    *   object {
    *     key1 = value1
    *     key2 = value2
    *   }
    *   object2 {
    *     key1 = value1
    * }
    * And contains the following members:
    *  Within the "topics" object:
    *    "alpha" (optional) : a dirichlet hyperparameter of the clumpiness of the model. Defaults to 1/e
    *    "beta" (optional) : a Double that specifies the value of beta. Defaults to 0.01
    *    "computeCoherence"  : a Boolean that specifies whether or not you would like to compute the coherence score of each topic
    *    "dateColumn" : a String that specifies the column in which to find the date
    *    "endDate" : a String that specifies the end (inclusive) of the date range you are running this job over
    *    "idColumn" : a String that specifies the column in which to find the id
    *    "iterN" (optional) : a String that specifies the number of iterations of MCMC sampling to run. Defaults to 150
    *    "k" (optional) : a String that specifies the number of topics to start with. Defaults to 2
    *    "numTopTopics" : a String that specifies the top T words which to consider in computing coherence scores
    *    "pathToCorpus" : a String that specifies the path to the data (on which to do the topic modelling)
    *    "pathToTfidf" (optional) : a String that specifies the path to precomputed tfidf scores. Job does not compute tfidf if pathToTfidf is unspecified
    *    "startDate" : a String that specifies beginning (inclusive) of the date range you are running this job over
    *    "pathToStopwords" : an Array of Strings that specifies the path to the various stopword files
    *    "textColumn" : a String that specifies column in which to find the text column
    *    "pathToWrite" : a String that specifies the path to the data (on which to do the topic modelling)
    *  Within the "spark" object:
    *    "master" : a String value that specifies the master node of this spark instance
    *    "app.name" : a String value that specifies this spark app's name
    *
    *  Notes:
    *  pathToTfidf:
    *  If you wish to provide precomputed tfidf scores, they must be in a csv file that adheres to the following schema:
    *  val schema = StructType(
    *    StructField("date", StringType, false) ::
    *    StructField("word", StringType , false) ::
    *    StructField("score", DoubleType, false) ::
    *    Nil
    *  )
    *
    * For example:
    * topics {
    *   beta = 0.01
    *   computeCoherence = true
    *   dateColumn = date
    *   stopWordFiles = [
    *     src/test/resources/topic-modelling/stopwords/stopwords_all_en.v2.txt,
    *     src/test/resources/topic-modelling/stopwords/stopwords_html_tags.txt
    *   ]
    *   ...
    * }
    * spark {
    *   master = local
    *   ...
    * }
    *
    * @param args Array of commandline arguments
    */
  def main(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length != 1) {
      logger.error("Usage: <job-executable> <config-file>")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config: Config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader()).resolve()
    val sqlContext: SQLContext = SparkConfig(config)
    val params: TopicModellingParams = TopicModellingConfigParser.parse(config)

    val stopwords_bcst = sqlContext.sparkContext.broadcast(params.stopwords)

    // Write results to file if 'outfile' specified in config
    val outputOperation = JobUtil.createTopicsOutputOperation(params.pathToWrite)

    val reader_corpus = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")

    val reader_tfidf = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")

    try {
      val topicModellingOp = doTopicModelling(
        params.alpha,
        params.beta,
        params.computeCoherence,
        params.dateCol,
        params.endDate,
        params.idCol,
        params.iterN,
        params.k,
        params.numTopTopics,
        params.pathToWrite,
        sqlContext,
        params.startDate,
        stopwords_bcst,
        params.textCol
      )(_ : (DataFrame, DataFrame))
      val corpus = Pipe(reader_corpus.load(params.pathToCorpus))
      val tfidf = Pipe(reader_tfidf.load(params.pathToTfidf))
      val merge = Pipe(corpus, tfidf)
        .to(topicModellingOp)
        .maybeTo(outputOperation)
        .run()

        // Without tfidf
        // val topicModellingOp = doTopicModelling(params.alpha, params.beta, params.computeCoherence, params.dateCol, params.endDate, params.idCol, params.iterN, params.k, params.numTopTopics, params.pathToWrite, sqlContext, params.startDate, stopwords_bcst, params.textCol, None)(_ : DataFrame)
        //
        // Pipe(reader_corpus.load(params.pathToCorpus))
        //   .to(topicModellingOp)
        //   .maybeTo(outputOperation)
        //   .run()

    } finally {
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      sqlContext.sparkContext.stop()
    }
  }
}
