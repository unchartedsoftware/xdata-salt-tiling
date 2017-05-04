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

/**
  * Provides functions for parsing word dictionary data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *   - `predefined` - This string parameter points to a file containing a list of words
  *     to use as the dictionary when calculating LDA scores. Not yet implemented for TFIDF. [OPTIONAL]
  *   - `stopwords` - This string parameter points to a file containing a list of stop-words to ignore
  *   when calculating LDA scores.  Not yet implemented for TFIDF. [OPTIONAL]
  *   - `maxDf` - This double parameter causes words in more than the
  *     given percentage of documents to be ignored, removed from the dictionary, when calculating TF*IDF or LDA scores.  This
  *     has the effect, essentially, of creating a default stopwords list based on the contents of the documents. [OPTIONAL]
  *   - `minDf` - This double parameter causes words in fewer than the given percentage of documents to
  *     be ignored, removed from the dictionary, when calculating TFIDF or LDA scores.  This has the effect of
  *     ignoring exceedingly rare words, that might appear in only one document. [OPTIONAL]
  *   - `maxFeatures` - Optional integer parameter defines the maximum number of words to retain in the dictionary.
  *     High-frequency words will be preffered over low-frequency words, so this parameter should probably be used
  *     in conjunction with `stopwords` or `maxDf`. [OPTIONAL]
  *   - `caseSensitivity` Boolean parameter, if true, indicates that the calculation of LDA or TFIDF scores is case-sensitive.
  *     False means it is case-insensitive.  Default is false.  [OPTIONAL]
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  dictionary {
  *    stopwords = ../stopwords.txt
  *    minDf = 1.0
  *    maxDf = 80.0
  *  }
  *  }}}
  *
  */
object DictionaryConfigParser extends ConfigParser {
  private val RootKey = "dictionary"
  private val PredefinedKey = "predefined"
  private val StopWordsKey = "stopwords"
  private val MaxDfKey = "maxDf"
  private val MinDfKey = "minDf"
  private val MaxFeaturesKey = "maxFeatures"
  private val CaseSensitivityKey = "caseSensitive"

  private val CaseSensitivityDefault = false

  /**
    * Parses word dictionary parameters out of a config container and instantiates a `Dictionary`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `S3OutputConfig` object.
    */
  def parse (config: Config): DictionaryConfig = {
    val section = getConfigOption(config, RootKey)

    val caseSensitive = section.flatMap(s =>
      getBooleanOption(s, CaseSensitivityKey)
    ).getOrElse(CaseSensitivityDefault)
    val predefined    = section.flatMap(s => getStringOption(s, PredefinedKey))
    val stopwords     = section.flatMap(s => getStringOption(s, StopWordsKey))
    val maxDF         = section.flatMap(s => getDoubleOption(s, MaxDfKey))
    val minDF         = section.flatMap(s => getDoubleOption(s, MinDfKey))
    val maxFeatures   = section.flatMap(s => getIntOption(s, MaxFeaturesKey))

    DictionaryConfig(caseSensitive, predefined, stopwords, maxDF, minDF, maxFeatures)
  }
}
