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
object LDAConfig extends ConfigParsingHelper {
  val LDA_ROOT_KEY = "lda"
  val NUM_TOPICS_KEY = "topics"
  val WORDS_PER_TOPIC_KEY = "words-per-topic"
  val TOPICS_PER_DOC_KEY = "topics-per-document"
  val CHECKPOINT_INTERVAL_KEY = "checkpoint-interval"
  val MAX_ITERATIONS_KEY = "maximum-iterations"
  val SEPARATOR_TOPIC_KEY = "output-separators"
  val TOPIC_SEPARATOR_KEY = "topic"
  val WORD_SEPARATOR_KEY = "word"
  val SCORE_SEPARATOR_KEY = "score"

  val DEFAULT_TOPIC_SEPARATOR = "\t"
  val DEFAULT_WORD_SEPARATOR = "|"
  val DEFAULT_SCORE_SEPARATOR = "="

  def apply (config: Config): Try[LDAConfig] = {
    Try {
      val topicsNode = config.getConfig(LDA_ROOT_KEY)
      val separatorsNode = optionalConfig(topicsNode, SEPARATOR_TOPIC_KEY)
      val (topicSeparator, wordSeparator, scoreSeparator) =
        separatorsNode.map(node => (
          optionalString(node, TOPIC_SEPARATOR_KEY).getOrElse(DEFAULT_TOPIC_SEPARATOR),
          optionalString(node, WORD_SEPARATOR_KEY).getOrElse(DEFAULT_WORD_SEPARATOR),
          optionalString(node, SCORE_SEPARATOR_KEY).getOrElse(DEFAULT_SCORE_SEPARATOR)
          )).getOrElse(
          (DEFAULT_TOPIC_SEPARATOR, DEFAULT_WORD_SEPARATOR, DEFAULT_SCORE_SEPARATOR)
        )
      LDAConfig(
        topicsNode.getInt(NUM_TOPICS_KEY),
        topicsNode.getInt(WORDS_PER_TOPIC_KEY),
        topicsNode.getInt(TOPICS_PER_DOC_KEY),
        optionalInt(topicsNode, CHECKPOINT_INTERVAL_KEY),
        optionalInt(topicsNode, MAX_ITERATIONS_KEY),
        topicSeparator, wordSeparator, scoreSeparator
      )
    }
  }
}
