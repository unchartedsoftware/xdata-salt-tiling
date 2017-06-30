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

package software.uncharted.contrib.tiling.config

import com.typesafe.config.Config
import software.uncharted.sparkpipe.ops.text.analytics.LDAConfig

import scala.util.Try


/**
  * Provides functions for parsing LDA data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  * - `topics` - The number of topics the LDA analysis is instructed to find across the entire corpus
  * - `wordsPerTopic` - The number of words the LDA analysis should use per topic
  * - `topicsPerDocument` - The number of topics the LDA analysis should assume per document
  * - `maximumIterations` - An optional number of maximum iterations to use when performing LDA analysis [OPTIONAL]
  * - `chkptInterval` - The number of iterations to perform between checkpoints, if checkpoints can be taken. [OPTIONAL]
  * - `topicSeparator` - A separator to use between topics when outputting LDA results [OPTIONAL] defaults to `\t`
  * - `wordSeparator` - A separator to use between word/score pairs when outputting LDA results [OPTIONAL] defaults to "|"
  * - `scoreSeparator` - A separator to use between a word and its score when outputting LDA results [OPTIONAL] defaults to "="
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  lda {
  *    topics = 100
  *    wordsPerTopic = 5
  *    topicsPerDocument = 5
  *  }
  *  }}}
  *
  */
object LDAConfigParser extends ConfigParser {
  private val RootKey = "lda"
  private val NumTopicsKey = "topics"
  private val WordsPerTopicKey = "wordsPerTopic"
  private val TopicsPerDocKey = "topicsPerDocument"
  private val CheckpointIntervalKey = "checkpointInterval"
  private val MaxIterationsKey = "maximumIterations"
  private val SeparatorTopicKey = "outputSeparators"
  private val TopicSeparatorKey = "topic"
  private val WordSeparatorKey = "word"
  private val ScoreSeparatorKey = "score"

  val DefaultTopicSeparator = "\t"
  val DefaultWordSeparator = "|"
  val DefaultScoreSeparator = "="

  /**
    * Parse LDA parameters out of a config container and instantiates a `LDAConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `LDAConfig` object.
    */
  def parse (config: Config): Try[LDAConfig] = {
    Try {
      val topicsNode = config.getConfig(RootKey)
      val separatorsNode = getConfigOption(topicsNode, SeparatorTopicKey)
      val (topicSeparator, wordSeparator, scoreSeparator) =
        separatorsNode.map(node => (
          getString(node, TopicSeparatorKey, DefaultTopicSeparator),
          getString(node, WordSeparatorKey, DefaultWordSeparator),
          getString(node, ScoreSeparatorKey, DefaultScoreSeparator)
          )).getOrElse(
          (DefaultTopicSeparator, DefaultWordSeparator, DefaultScoreSeparator)
        )

      LDAConfig(
        topicsNode.getInt(NumTopicsKey),
        topicsNode.getInt(WordsPerTopicKey),
        topicsNode.getInt(TopicsPerDocKey),
        getIntOption(topicsNode, CheckpointIntervalKey),
        getIntOption(topicsNode, MaxIterationsKey),
        topicSeparator, wordSeparator, scoreSeparator
      )
    }
  }
}
