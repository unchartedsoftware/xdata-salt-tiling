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
  * Configuration specifying how tile based TF*IDF is to be performed
  *
  * @param tf The algorithm to perform the term-frequency calculation
  * @param idf The algorithm to perform the inverse-document-frequency calculation
  * @param dictionaryConfig Settings associated with the corpus dictionary
  * @param wordsToKeep Number of words to retain per tile, sorted by computed score
  */
case class TFIDFConfig(tf: TFType,
                       idf: IDFType,
                       dictionaryConfig: DictionaryConfig,
                       wordsToKeep: Int)

/**
  * Provides functions for parsing Tile-based TFIDF data out of `com.typesafe.config.Config` objects.
  *
  * Valid properties are:
  *
  *   - `tf` - The function used to compute term frequency - one of `binary`, `raw`, `normalized`, `sublinear` or
  *            `double-normalized`.  [OPTIONAL] defaults to `sublinear`.`
  *   - `idf` - The function used to compute document frequency - one of `unary`, `linear`, `log`, `log-smooth` or
  *             `probabilistic`.  [OPTIONAL] defaults to `log-smooth`.
  *   - `k` - Additional `k` value required when computing `double-normalized` TF.
  *   - `words` - Number of terms to retain per tile.  Terms are sorted by score,selection is top N.
  *   - `dictionary` - A `DictionaryConfig` block - see docs for that class for details.
  *
  *  Example from config file (in [[https://github.com/typesafehub/config#using-hocon-the-json-superset HOCON]] notation):
  *
  *  {{{
  *  tfidf {
  *    tf = raw
  *    df = log
  *    words = 10
  *    dictionary = {
  *      minDf = 0.1
  *    }
  *  }
  *  }}}
  *
  */
object TFIDFConfigParser extends ConfigParser {
  private val RootKey = "tf-idf"
  private val TypeKey = "type"

  // Keys by which our variations of term-frequency should be known
  private val TfTypeBinary = "binary"
  private val TfTypeRaw = "raw"
  private val TfTypeNormalized = "normalized"
  private val TfTypeSublinear = "sublinear"
  private val TfTypeDoubleNormalized = "double-normalized"

  // Parameter for double-normalized TF - see that class for details
  private val TfDoubleNormalizedKey = "k"

  // Default term-frequency calculation type
  private val DefaultTfTypeName = TfTypeSublinear
  private val DefaultTfType = analytics.SublinearTF

  // Keyed variations in how inverse-document-frequency should be calculated
  private val IdfTypes = Map(
    "unary" -> analytics.UnaryIDF,
    "linear" -> analytics.LinearIDF,
    "log" -> analytics.LogIDF,
    "log-smooth" -> analytics.SmoothLogIDF,
    "probabalistic" -> analytics.ProbabilisticIDF
  )
  // Default inverse-document-frequency calculation type
  private val DefaultIdfTypeName = "log-smooth"

  // Key pointing to the number of words TF*IDF should keep per record
  private val WordsToKeep = "words"

  // Read the term frequency type
  private def tfConfig(config: Config): Try[analytics.TFType] = {
    Try {
      getConfigOption(config, RootKey).map { tfIdfSection =>
        getString(tfIdfSection, TypeKey, DefaultTfTypeName).toLowerCase.trim match {
          case TfTypeBinary => analytics.BinaryTF.asInstanceOf
          case TfTypeRaw => analytics.RawTF
          case TfTypeNormalized => analytics.DocumentNormalizedTF
          case TfTypeSublinear => analytics.SublinearTF
          case TfTypeDoubleNormalized =>
            val k = getDoubleOption(tfIdfSection, TfDoubleNormalizedKey).get
            analytics.TermNormalizedTF(k)
        }
      }.getOrElse(DefaultTfType)
    }
  }

  // Read the inverse document frequency type
  private def idfConfig(config: Config): analytics.IDFType = {
    getConfigOption(config, RootKey).flatMap { tfIdfSection =>
        IdfTypes.get(getString(tfIdfSection, TypeKey, DefaultIdfTypeName).toLowerCase.trim)
    }.getOrElse(IdfTypes(DefaultIdfTypeName))
  }

  /**
    * Parses general tiling parameters out of a config container and instantiates a `TFIDFConfig`
    * object from them.
    *
    * @param config The configuration container.
    * @return A `Try` containing the `TFIDFConfig` object.
    */
  def parse(config: Config): Try[TFIDFConfig] = {
    tfConfig(config).map { tfConf =>
      val section = config.getConfig(RootKey)
      val idfConf = idfConfig(config)
      val dictConf = DictionaryConfigParser.parse(section)
      val wordsToKeep = section.getInt(WordsToKeep)

      TFIDFConfig(tfConf, idfConf, dictConf, wordsToKeep)
    }
  }
}
