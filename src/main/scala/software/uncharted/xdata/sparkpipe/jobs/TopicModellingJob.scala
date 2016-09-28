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
import org.apache.spark.sql.SQLContext
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.topics.topicModelling.doTopicModelling
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TopicModellingConfigParser, TopicModellingParams}

// scalastyle:off method.length parameter.number
object TopicModellingJob extends Logging {

  /**
    * Entrypoint
    *
    * @param args Array of commandline arguments
    */
  def main(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length != 1) {
      logger.error("Usage: <topic-modelling-executable> <config-file>")
      sys.exit(-1)
    }

    // load properties file from supplied URI
    val config: Config = ConfigFactory.parseReader(scala.io.Source.fromFile(args(0)).bufferedReader()).resolve()
    val sqlContext: SQLContext = SparkConfig(config)
    val sparkContext: SparkContext = sqlContext.sparkContext
    val params: TopicModellingParams = TopicModellingConfigParser.parse(config, sparkContext)

    // Parse Errors?
    val reader = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")

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
      params.outdir,
      params.path,
      params.startDate,
      params.stopwords_bcst,
      params.textCol,
      params.tfidf_bcst
    )(_)

    try {
      Pipe(reader.load(params.path))
        .to(topicModellingOp).run()
    } finally {
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      sparkContext.stop()
    }
  }
}
