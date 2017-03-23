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
package software.uncharted.xdata.sparkpipe.config

import com.typesafe.config.Config

import scala.util.Try

/**
  * A configuration describing LDA-specific parameters to using when performing LDA
  *
  * @param numTopics The number of topics the LDA analysis is instructed to find
  * @param wordsPerTopic The number of words the LDA analysis should use per topic
  * @param topicsPerDocument The number of topics the LDA analysis should assume per document
  * @param maxIterations An optional number of maximum iterations to use when performing LDA analysis
  * @param chkptInterval The number of iterations to perform between checkpoints, if checkpoints can be taken.
  * @param topicSeparator A separator to use between topics when outputting LDA results
  * @param wordSeparator A separator to use between word/score pairs when outputting LDA results
  * @param scoreSeparator A separator to use between a word and its score when outputting LDA results
  */
case class LDAConfig (numTopics: Int,
                      wordsPerTopic: Int,
                      topicsPerDocument: Int,
                      chkptInterval: Option[Int],
                      maxIterations: Option[Int],
                      topicSeparator: String,
                      wordSeparator: String,
                      scoreSeparator: String)
object LDAConfig extends ConfigParser {
  private val ldaRoot = "lda"
  private val numTopics = "topics"
  private val wordsPerTopic = "words-per-topic"
  private val topicsPerDoc = "topics-per-document"
  private val checkpointInterval = "checkpoint-interval"
  private val maxIterations = "maximum-iterations"
  private val separatorTopic = "output-separators"
  private val topicSeparatorKey = "topic"
  private val wordSeparatorKey = "word"
  private val scoreSeparatorKey = "score"

  val defaultTopicSeparator = "\t"
  val defaultWordSeparator = "|"
  val defaultScoreSeparator = "="

  def parse (config: Config): Try[LDAConfig] = {
    Try {
      val topicsNode = config.getConfig(ldaRoot)
      val separatorsNode = getConfigOption(topicsNode, separatorTopic)
      val (topicSeparator, wordSeparator, scoreSeparator) =
        separatorsNode.map(node => (
          getString(node, topicSeparatorKey, defaultTopicSeparator),
          getString(node, wordSeparatorKey, defaultWordSeparator),
          getString(node, scoreSeparatorKey, defaultScoreSeparator)
          )).getOrElse(
          (defaultTopicSeparator, defaultWordSeparator, defaultScoreSeparator)
        )

      LDAConfig(
        topicsNode.getInt(numTopics),
        topicsNode.getInt(wordsPerTopic),
        topicsNode.getInt(topicsPerDoc),
        getIntOption(topicsNode, checkpointInterval),
        getIntOption(topicsNode, maxIterations),
        topicSeparator, wordSeparator, scoreSeparator
      )
    }
  }
}
