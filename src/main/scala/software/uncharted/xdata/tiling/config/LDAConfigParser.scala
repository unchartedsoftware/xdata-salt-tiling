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
package software.uncharted.xdata.tiling.config

import com.typesafe.config.Config
import software.uncharted.sparkpipe.ops.xdata.text.analytics.LDAConfig

import scala.util.Try


object LDAConfigParser extends ConfigParser {
  private val LDA_ROOT_KEY = "lda"
  private val NUM_TOPICS_KEY = "topics"
  private val WORDS_PER_TOPIC_KEY = "wordsPerTopic"
  private val TOPICS_PER_DOC_KEY = "topicsPerDocument"
  private val CHECKPOINT_INTERVAL_KEY = "checkpointInterval"
  private val MAX_ITERATIONS_KEY = "maximumIterations"
  private val SEPARATOR_TOPIC_KEY = "outputSeparators"
  private val TOPIC_SEPARATOR_KEY = "topic"
  private val WORD_SEPARATOR_KEY = "word"
  private val SCORE_SEPARATOR_KEY = "score"

  val DEFAULT_TOPIC_SEPARATOR = "\t"
  val DEFAULT_WORD_SEPARATOR = "|"
  val DEFAULT_SCORE_SEPARATOR = "="

  def parse (config: Config): Try[LDAConfig] = {
    Try {
      val topicsNode = config.getConfig(LDA_ROOT_KEY)
      val separatorsNode = getConfigOption(topicsNode, SEPARATOR_TOPIC_KEY)
      val (topicSeparator, wordSeparator, scoreSeparator) =
        separatorsNode.map(node => (
          getString(node, TOPIC_SEPARATOR_KEY, DEFAULT_TOPIC_SEPARATOR),
          getString(node, WORD_SEPARATOR_KEY, DEFAULT_WORD_SEPARATOR),
          getString(node, SCORE_SEPARATOR_KEY, DEFAULT_SCORE_SEPARATOR)
          )).getOrElse(
          (DEFAULT_TOPIC_SEPARATOR, DEFAULT_WORD_SEPARATOR, DEFAULT_SCORE_SEPARATOR)
        )

      LDAConfig(
        topicsNode.getInt(NUM_TOPICS_KEY),
        topicsNode.getInt(WORDS_PER_TOPIC_KEY),
        topicsNode.getInt(TOPICS_PER_DOC_KEY),
        getIntOption(topicsNode, CHECKPOINT_INTERVAL_KEY),
        getIntOption(topicsNode, MAX_ITERATIONS_KEY),
        topicSeparator, wordSeparator, scoreSeparator
      )
    }
  }
}
