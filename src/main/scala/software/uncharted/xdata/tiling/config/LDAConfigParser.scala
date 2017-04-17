/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
