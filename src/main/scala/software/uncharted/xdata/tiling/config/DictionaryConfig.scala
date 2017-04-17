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
import software.uncharted.sparkpipe.ops.xdata.text.analytics.DictionaryConfig

object DictionaryConfigParser extends ConfigParser {
  private val DICTIONARY_SECTION = "dictionary"
  // Key for the vocabulary parameter.  This optional string parameter points to a file containing a list of words to
  // use as the dictionary when calculating TF*IDF scores.
  // Note, however, this functionality is currently unimplemented
  // If it were implemented, it being present should be incompatible with any other parameter being present.
  private val DICTIONARY_KEY = "predefined"
  // Key for the stopwords parameter.  This optional string parameter points to a file containing a list of stop-words
  // to ignore when calculating TF*IDF scores.
  // Note, however, this functionality is currently unimplemented
  private val STOPWORDS_KEY = "stopwords"
  // Key for the maximum document frequency parameter.  This optional double parameter causes words in more than the
  // given percentage of documents to be ignored, removed from the dictionary, when calculating TF*IDF scores.  This
  // has the effect, essentially, of creating a default stopwords list based on the contents of the documents.
  private val MAX_DF_KEY = "max-df"
  // Key for the minimum document frequency parameter.  This optional double parameter causes words in fewer than the
  // given percentage of documents to be ignored, removed from the dictionary, when calculating TF*IDF scores.  This
  // has the effect of ignoring exceedingly rare words, that might appear in only one document.
  private val MIN_DF_KEY = "min-df"
  // Key for the maximum number of words to retain in the dictionary.  This optional integer parameter defines the
  // maximum number of words to retain in the dictionary.  High-frequency words will be preffered over low-frequency
  // words, so this parameter should probably be used in conjunction with stopwords or max-df.
  private val MAX_FEATURES_KEY = "max-features"
  // Key for the case sensitivity of our calculation.  This boolean parameter, if true, indicates that the calculation
  // of TF*IDF scores is case-sensitive.  False means it is case-insensitive.  Default is false.
  private val CASE_SENSITIVITY_KEY = "case-sensitive"
  private val CASE_SENSITIVITY_DEFAULT = false


  def parse (config: Config): DictionaryConfig = {
    val section = getConfigOption(config, DICTIONARY_SECTION)

    val caseSensitive = section.flatMap(s =>
      getBooleanOption(s, CASE_SENSITIVITY_KEY)
    ).getOrElse(CASE_SENSITIVITY_DEFAULT)
    val predefined    = section.flatMap(s => getStringOption(s, DICTIONARY_KEY))
    val stopwords     = section.flatMap(s => getStringOption(s, STOPWORDS_KEY))
    val maxDF         = section.flatMap(s => getDoubleOption(s, MAX_DF_KEY))
    val minDF         = section.flatMap(s => getDoubleOption(s, MIN_DF_KEY))
    val maxFeatures   = section.flatMap(s => getIntOption(s, MAX_FEATURES_KEY))

    DictionaryConfig(caseSensitive, predefined, stopwords, maxDF, minDF, maxFeatures)
  }
}
