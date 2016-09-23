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
import software.uncharted.xdata.ops.topics.TopicModelling
import software.uncharted.xdata.sparkpipe.config.{SparkConfig, TopicModellingConfigParser, TopicModellingParams}

// scalastyle:off method.length parameter.number
object TopicModellingParallelJob extends Logging {

  /*
  val path = "/xdata/data/twitter/isil-keywords/2016-09/isil_keywords.2016090 "
  val dates = Array("2016-09-03", "2016-09-04", "2016-09-05")
  val bdp = RunBDPParallel(sc)
  val rdd = bdp.loadTSVTweets(path, dates, 1, 0, 6)
   */

  /**
    * Entrypoint
    *
    * @param args Array of commandline arguments
    */
  def main(args: Array[String]): Unit = {
    // get the properties file path
    if (args.length != 1) {
      logger.error("Usage: ") // TODO
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

    val topicModellingOp = TopicModelling.learnTopicsParallel(
      params.startDate,
      params.endDate,
      params.stopwords_bcst,
      params.iterN,
      params.k,
      params.alpha,
      params.eta,
      params.outdir,
      params.weighted,
      params.tfidf_bcst,
      params.hdfspath, // TODO rename to path
      params.caIdx,
      params.idIdx,
      params.textIdx
    )(_)

    try {
      // Create the dataframe from the input config
//      val df = params.rdd.toDF
//      val df = dataframeFromSparkCsv(config, tilingConfig.source, schema, sqlc)
      // Pipe the dataframe
      Pipe(reader.load(params.hdfspath))
        .to(topicModellingOp).run()

      // create and save extra level metadata - the tile x,y,z dimensions in this case
      // writeMetadata(config, tilingConfig, heatmapConfig)

    } finally {
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      sparkContext.stop()
    }
  }
}
