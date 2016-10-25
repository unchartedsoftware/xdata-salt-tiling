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
import grizzled.slf4j.{Logger, Logging}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.text.LDAOp
import software.uncharted.xdata.sparkpipe.config.{HdfsCsvConfig, HdfsIOConfig, LDAConfig}

import scala.util.{Failure, Success}

/**
  * A job that augments a csv-like dataset with a new column representing the LDA-derived topics in that dataset
  */
object LDAAugmentationJob extends AbstractJob {
  private def readInputConfig (config: Config): HdfsCsvConfig = {
    HdfsIOConfig.csv("input")(config) match {
      case Success(config) => {
        if (config.neededColumns.length != 1) {
          logger.error("Input configuration specifies other than 1 column")
          sys.exit(-1)
        }
        config
      }
      case Failure(e) =>
        logger.error("Error reading input config", e)
        sys.exit(-1)
    }
  }

  private def readOutputConfig (config: Config): HdfsCsvConfig = {
    HdfsIOConfig.csv("output")(config) match {
      case Success(config) => config
      case Failure(e) =>
        logger.error("Error reading output configuration", e)
        sys.exit(-1)
    }
  }

  private def readLDAConfig (config: Config): LDAConfig = {
    LDAConfig(config) match {
      case Success(config) => config
      case Failure(e) =>
        logger.error("Error reading LDA configuration", e)
        sys.exit(-1)
    }
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param sqlc   An SQL context in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(sqlc: SQLContext, config: Config): Unit = {
    config.resolve()
    // Ignore info messages
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

    val inputConfig = readInputConfig(config)
    val outputConfig = readOutputConfig(config)
    val ldaConfig = readLDAConfig(config)

    // Read data
    val inputData = readFile(sqlc.sparkContext, inputConfig).zipWithIndex().map { case ((rawRecord, fields), index) =>
      val text = fields(0)
      (index, (rawRecord, text))
    }

    // Pull out the text column
    val texts = inputData.map { case (id, (rawRecord, text)) => (id, text) }
    dbg("(1) There are " + texts.count + " texts")
    // Perform LDA on the text column
    val topics = LDAOp.lda(ldaConfig.numTopics, ldaConfig.wordsPerTopic, ldaConfig.topicsPerDocument,
                           ldaConfig.maxIterations, ldaConfig.chkptInterval)(texts)
    dbg("(2) There are " + topics.count + " topic records")
    // Reformat topics for output
    val formattedTopics = topics.map { case (docId, topics) =>
      (
        docId,
        topics.map { entry =>
          entry.topic.map(wordScore => wordScore.word + ldaConfig.scoreSeparator + wordScore.score)
            .mkString(ldaConfig.wordSeparator) + ldaConfig.wordSeparator + entry.score
        }.mkString(ldaConfig.topicSeparator)
        )
    }
    dbg("(3) There are " + formattedTopics.count + " formatted topic records")
    // Join the LDA results back in
    val joinedData = inputData.join(formattedTopics)
    dbg("(4) There are " + joinedData.count + " joined records")
    // TODO: Sort or not based upon an input parameter
    val sortedData = joinedData.sortBy(_._1)
    dbg("(5) There are " + sortedData.count + " sorted recoreds")
    val output = sortedData.map { case (id, ((rawRecord, text), topics)) =>
      rawRecord + inputConfig.separator + topics
    }

    // Replace separators if necessary
    val toWrite = reformatOutput(output, inputConfig, outputConfig)
    dbg("(6) There are " + toWrite.count + " records to write\n" + "Writing to " + outputConfig.location)
    // Write out the data
    toWrite.saveAsTextFile(outputConfig.location)
  }

  private def reformatOutput (startingOutput: RDD[String],
                              inputConfig: HdfsCsvConfig,
                              outputConfig: HdfsCsvConfig): RDD[String] = {
    val reseparated =
      if (outputConfig.separator == inputConfig.separator) {
        startingOutput
      } else {
        startingOutput.map(line => line.split(inputConfig.separator).mkString(outputConfig.separator))
      }
    outputConfig.partitions.map(partitions => reseparated.repartition(partitions)).getOrElse(reseparated)
  }

  // scalastyle:off regex
  def dbg (msg: String): Unit = {
    println
    println
    println
    println(msg)
    println
    println
    println
  }
  // scalastyle:on regex

  def readFile (sc: SparkContext, config: HdfsCsvConfig): RDD[(String, Seq[String])] = {
    sc.textFile(config.location).map{line =>
      val fields = line.split(config.separator)
      val relevantFields = config.neededColumns.map(n => fields(n))
      (line, relevantFields)
    }
  }
}
