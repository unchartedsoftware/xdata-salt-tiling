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
import software.uncharted.sparkpipe.ops.xdata.text.analytics
import software.uncharted.sparkpipe.ops.xdata.text.analytics.{DictionaryConfig, IDFType, TFType}

import scala.util.Try


/**
  * Configuration specifying how TF*IDF is to be performed
  *
  * @param tf The algorithm to perform the term-frequency calculation
  * @param idf The algorithm to perform the inverse-document-frequency calculation
  */
case class TFIDFConfig(tf: TFType,
                       idf: IDFType,
                       dictionaryConfig: DictionaryConfig,
                       wordsToKeep: Int)

object TFIDFConfigParser extends ConfigParser {
  private val SECTION_KEY = "tf-idf"
  private val TYPE_KEY = "type"

  // Primary section title for term-frequency section
  private val TF_SECTION_KEY_1 = "term-frequency"
  // Shortened section title for term-frequency section
  private val TF_SECTION_KEY_2 = "tf"
  // Keys by which our variations of term-frequency should be known
  private val TF_TYPE_BINARY = "binary"
  private val TF_TYPE_RAW = "raw"
  private val TF_TYPE_NORMALIZED = "normalized"
  private val TF_TYPE_SUBLINEAR = "sublinear"
  private val TF_TYPE_DOUBLE_NORMALIZED = "double-normalized"

  // Parameter for double-normalized TF - see that class for details
  private val TF_DOUBLE_NORMALIZED_K_KEY = "k"

  // Default term-frequency calculation type
  private val DEFAULT_TF_TYPE_NAME = TF_TYPE_SUBLINEAR
  private val DEFAULT_TF_TYPE = analytics.SublinearTF

  // Primary section title for inverse-document-frequency section
  private val IDF_SECTION_KEY_1 = "inverse-document-frequency"
  // Shortened section title for inverse-document-frequency section
  private val IDF_SECTION_KEY_2 = "idf"
  // Keyed variations in how inverse-document-frequency should be calculated
  private val IDF_TYPES = Map(
    "unary" -> analytics.UnaryIDF,
    "linear" -> analytics.LinearIDF,
    "log" -> analytics.LogIDF,
    "log-smooth" -> analytics.SmoothLogIDF,
    "probabalistic" -> analytics.ProbabilisticIDF
  )
  // Default inverse-document-frequency calculation type
  private val DEFAULT_IDF_TYPE_NAME = "log-smooth"

  // Key pointing to the number of words TF*IDF should keep per record
  private val WORDS_TO_KEEP = "words"

  // Read the term frequency type
  private def tfConfig(config: Config): Try[analytics.TFType] = {
    Try {
      getConfigOption(config, SECTION_KEY).map { tfIdfSection =>
        getConfigOption(config, TF_SECTION_KEY_1, TF_SECTION_KEY_2).map { tfSection =>
          getString(tfSection, TYPE_KEY, DEFAULT_TF_TYPE_NAME).toLowerCase.trim match {
            case TF_TYPE_BINARY => analytics.BinaryTF.asInstanceOf
            case TF_TYPE_RAW => analytics.RawTF
            case TF_TYPE_NORMALIZED => analytics.DocumentNormalizedTF
            case TF_TYPE_SUBLINEAR => analytics.SublinearTF
            case TF_TYPE_DOUBLE_NORMALIZED =>
              val k = getDoubleOption(tfSection, TF_DOUBLE_NORMALIZED_K_KEY).get
              analytics.TermNormalizedTF(k)
          }
        }.getOrElse(DEFAULT_TF_TYPE)
      }.getOrElse(DEFAULT_TF_TYPE)
    }
  }

  // Read the inverse document frequency type
  private def idfConfig(config: Config): analytics.IDFType = {
    getConfigOption(config, SECTION_KEY).flatMap { tfIdfSection =>
      getConfigOption(config, IDF_SECTION_KEY_1, IDF_SECTION_KEY_2).flatMap { section =>
        IDF_TYPES.get(getString(section, TYPE_KEY, DEFAULT_IDF_TYPE_NAME).toLowerCase.trim)
      }
    }.getOrElse(IDF_TYPES(DEFAULT_IDF_TYPE_NAME))
  }


  def parse(config: Config): Try[TFIDFConfig] = {
    tfConfig(config).map { tfConf =>
      val section = config.getConfig(SECTION_KEY)

      val idfConf = idfConfig(config)
      val dictConf = DictionaryConfigParser.parse(section)
      val wordsToKeep = section.getInt(WORDS_TO_KEEP)

      TFIDFConfig(tfConf, idfConf, dictConf, wordsToKeep)
    }
  }
}
