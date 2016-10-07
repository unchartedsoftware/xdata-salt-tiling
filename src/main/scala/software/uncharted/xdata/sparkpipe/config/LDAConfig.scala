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
  * @param topicSeparator A separator to use between topics when outputting LDA results
  * @param wordSeparator A separator to use between word/score pairs when outputting LDA results
  * @param scoreSeparator A separator to use between a word and its score when outputting LDA results
  */
case class LDAConfig (numTopics: Int, wordsPerTopic: Int, topicsPerDocument: Int,
                      topicSeparator: String,
                      wordSeparator: String,
                      scoreSeparator: String)
object LDAConfig {
  val LDA_ROOT_KEY = "lda"
  val NUM_TOPICS_KEY = "topics"
  val WORDS_PER_TOPIC_KEY = "words-per-topic"
  val TOPICS_PER_DOC_KEY = "topics-per-document"
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
      val separatorsNode = optionallyGetSubconfig(topicsNode, SEPARATOR_TOPIC_KEY)
      val (topicSeparator, wordSeparator, scoreSeparator) =
        separatorsNode.map(node => (
          optionallyGetString(node, SEPARATOR_TOPIC_KEY, DEFAULT_TOPIC_SEPARATOR),
          optionallyGetString(node, WORD_SEPARATOR_KEY, DEFAULT_WORD_SEPARATOR),
          optionallyGetString(node, SCORE_SEPARATOR_KEY, DEFAULT_SCORE_SEPARATOR)
          )).getOrElse(
          (DEFAULT_TOPIC_SEPARATOR, DEFAULT_WORD_SEPARATOR, DEFAULT_SCORE_SEPARATOR)
        )
      LDAConfig(
        topicsNode.getInt(NUM_TOPICS_KEY),
        topicsNode.getInt(WORDS_PER_TOPIC_KEY),
        topicsNode.getInt(TOPICS_PER_DOC_KEY),
        topicSeparator, wordSeparator, scoreSeparator
      )
    }
  }

  private def optionallyGetSubconfig (config: Config, key: String): Option[Config] = {
    if (config.hasPath(key))
      Some(config.getConfig(key))
    else
      None
  }
  private def optionallyGetString (config: Config, key: String, defaultValue: String): String = {
    if (config.hasPath(key)) config.getString(key)
    else defaultValue
  }
}
