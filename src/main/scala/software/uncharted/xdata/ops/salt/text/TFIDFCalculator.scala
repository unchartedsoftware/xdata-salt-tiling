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
package software.uncharted.xdata.ops.salt.text

import com.typesafe.config.Config
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.util.Try



/*
 * Possible Additions
 *   vocabulary: Array[String]   use only words in this to compute TF/IDF
 *   stopword: Array[String]     ignore words in this
 *   max_df: Int                 ignore words above this threshold (automatically detect and filter stopwords)
 *   min_df: Int                 ignore words below this threshold
 *   max_features: Int           build a vocabulary using only the top max_features words
 *   lowercase: Boolean          optionally lowercase all words
 *
 *
 * Normalization:
 *   Sometimes a normalization factor is used to correct discrepencies in document lengths.
 *   Document vectors are normalized so that documents are retrieved independent of their lengths.
 *   If this is not done and lengths vary considerably, very short documents may not be recognized as being relevant.
 *   Cosine Normalization:
 *     1 / math.sqrt( math.pow(w1, 2) + math.pow(w2, 2) +...+ math.pow(wn, 2) )
 *
 *     where w is the weight of term i
 *
 *
 * TFIDF calculation
 *   should there be a TFIDFCalculator object to compute:  tf * idf
 *
 *
 * IDF
 *   maybe refactor to use two flags:
 *   smooth_idf: Boolean     whether or not to add +1 smoothing
 *   log_idf: Boolean        whether or not to apply log(IDF)
 */



sealed trait TFType extends Serializable {
  /** Calculate the term frequency of a given term in a given document.
    * Note that neither the term in question or the document are actually given, just statistically described.
    *
    * @param rawF The number of times the given term appears in the current document
    * @param nTerms The number of terms in the current document
    * @param maxRawF The maximum number of times any given term appears in the current document
    * @return The TF score for this term in this document
    */
  def termFrequency (rawF: Int, nTerms: Int, maxRawF: Int): Double
}

/**
  * Binary term frequency - 1 if term is present in the current document, 0 if it is absent
  */
object BinaryTF extends TFType {
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    if (rawF > 0) {
      1
    } else {
      0
    }
  }
}

/**
  * Raw term frequency - just the count of the number of occurances of theterm in the current document
  */
object RawTF extends TFType {
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    rawF.toDouble
  }
}

/**
  * Term frequency, normalized by the size of the document
  */
object DocumentNormalizedTF extends TFType {
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    if (0 == nTerms) {
      0
    } else {
      rawF.toDouble / nTerms.toDouble
    }
  }
}

/**
  * Sublinear term frequency scaling. This is a variant of TF that goes beyond counting occurences shrinking the range
  * eoung so as to put everything into a comprehensible range. It captures the intuition that, for example, ten
  * occurrences of a term in a document is unlikely to carry ten times the significance
  * of a since occurence.
  */
object SublinearTF extends TFType {
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    if (rawF > 0) {
      1.0 + math.log(rawF)
    } else {
      // raw frequency of 0 should remain 0
      0
    }
  }
}

/**
  * Term frequency, normalized by the maximum term frequency in the current document (to prevent a preference for
  * longer documents)
  *
  * @param k The minimum allowed TF score.  K must be in the range [0, 1), and all TF scores will fall in the range
  *          [k, 1]
  */
case class TermNormalizedTF (k: Double) extends TFType {
  assert(0.0 <= k)
  assert(k < 1.0)
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    k + (1.0 - k) * rawF / maxRawF
  }
}



sealed trait IDFType extends Serializable {
  /** Calculate the inverse document frequency of a given term with respect to a given document.
    * Note that neither the term in question or the document are actually given, just statistically described.
    *
    * @param N The total number of documents
    * @param n_t The number of documents containing the current term
    * @return The inverse document frequency of the given term.
    */
  def inverseDocumentFrequency (N: Long, n_t: Int): Double
}

/**
  * Unary inverse document frequency - always 1
  */
object UnaryIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    1.0
  }
}

/**
  * Linear inverse document frequency - simply 1/document frequency
  */
object LinearIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    N.toDouble / n_t.toDouble
  }
}

/**
  * Log inverse document frequency - keep ranges smaller, more understandable through use of logs
  */
object LogIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    math.log(N.toDouble / n_t.toDouble)
  }
}

/**
  * Smooth log inverse document frequency - further shrinking of range from unsmoothed log by forcing frequencies
  * above one
  */
object SmoothLogIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    math.log(1.0 + N.toDouble / n_t.toDouble)
  }
}


/**
  * This variant of IDF assigns weights ranging from -infinity for a term that appears in every document
  * to log(n-1) for a term that appears in only one document. Thus, unlike log IDF  it assigns negative
  * weights to terms that appear in more than half of the documents.
  */
object ProbabilisticIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    math.log((N.toDouble - n_t.toDouble) / n_t.toDouble)
  }
}

object TFIDFCalculatorParser extends ConfigParser {
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
  private val DEFAULT_TF_TYPE = SublinearTF

  // Primary section title for inverse-document-frequency section
  private val IDF_SECTION_KEY_1 = "inverse-document-frequency"
  // Shortened section title for inverse-document-frequency section
  private val IDF_SECTION_KEY_2 = "idf"
  // Keyed variations in how inverse-document-frequency should be calculated
  private val IDF_TYPES = Map(
    "unary" -> UnaryIDF,
    "linear" -> LinearIDF,
    "log" -> LogIDF,
    "log-smooth" -> SmoothLogIDF,
    "probabalistic" -> ProbabilisticIDF
  )
  // Default inverse-document-frequency calculation type
  private val DEFAULT_IDF_TYPE_NAME = "log-smooth"

  // Key pointing to the number of words TF*IDF should keep per record
  private val WORDS_TO_KEEP = "words"

  // Read the term frequency type
  private def tfConfig(config: Config): Try[TFType] = {
    Try {
      getConfigOption(config, SECTION_KEY).map { tfIdfSection =>
        getConfigOption(config, TF_SECTION_KEY_1, TF_SECTION_KEY_2).map { tfSection =>
          getString(tfSection, TYPE_KEY, DEFAULT_TF_TYPE_NAME).toLowerCase.trim match {
            case TF_TYPE_BINARY => BinaryTF.asInstanceOf
            case TF_TYPE_RAW => RawTF
            case TF_TYPE_NORMALIZED => DocumentNormalizedTF
            case TF_TYPE_SUBLINEAR => SublinearTF
            case TF_TYPE_DOUBLE_NORMALIZED =>
              val k = getDoubleOption(tfSection, TF_DOUBLE_NORMALIZED_K_KEY).get
              TermNormalizedTF(k)
          }
        }.getOrElse(DEFAULT_TF_TYPE)
      }.getOrElse(DEFAULT_TF_TYPE)
    }
  }

  // Read the inverse document frequency type
  private def idfConfig(config: Config): IDFType = {
    getConfigOption(config, SECTION_KEY).flatMap { tfIdfSection =>
      getConfigOption(config, IDF_SECTION_KEY_1, IDF_SECTION_KEY_2).flatMap { section =>
        IDF_TYPES.get(getString(section, TYPE_KEY, DEFAULT_IDF_TYPE_NAME).toLowerCase.trim)
      }
    }.getOrElse(IDF_TYPES(DEFAULT_IDF_TYPE_NAME))
  }


  def tfIdfConfig(config: Config): Try[TFIDFConfiguration] = {
    tfConfig(config).map { tfConf =>
      val section = config.getConfig(SECTION_KEY)

      val idfConf = idfConfig(config)
      val dictConf = DictionaryConfigurationParser.parse(section)
      val wordsToKeep = section.getInt(WORDS_TO_KEEP)

      TFIDFConfiguration(tfConf, idfConf, dictConf, wordsToKeep)
    }
  }
}
/**
  * Configuration specifying how TF*IDF is to be performed
  *
  * @param tf The algorithm to perform the term-frequency calculation
  * @param idf The algorithm to perform the inverse-document-frequency calculation
  */
case class TFIDFConfiguration (tf: TFType,
                               idf: IDFType,
                               dictionaryConfig: DictionaryConfiguration,
                               wordsToKeep: Int)




object DictionaryConfigurationParser extends ConfigParser {
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


  def parse (config: Config): DictionaryConfiguration = {
    val section = getConfigOption(config, DICTIONARY_SECTION)

    val caseSensitive = section.flatMap(s =>
      getBooleanOption(s, CASE_SENSITIVITY_KEY)
    ).getOrElse(CASE_SENSITIVITY_DEFAULT)
    val predefined    = section.flatMap(s => getStringOption(s, DICTIONARY_KEY))
    val stopwords     = section.flatMap(s => getStringOption(s, STOPWORDS_KEY))
    val maxDF         = section.flatMap(s => getDoubleOption(s, MAX_DF_KEY))
    val minDF         = section.flatMap(s => getDoubleOption(s, MIN_DF_KEY))
    val maxFeatures   = section.flatMap(s => getIntOption(s, MAX_FEATURES_KEY))

    DictionaryConfiguration(caseSensitive, predefined, stopwords, maxDF, minDF, maxFeatures)
  }

}

/**
  * Configuration delineating how to construct a dictionary for use by a text analytic
  *
  * @param caseSensitive Whether or not dictionary determination should be case-sensitive
  * @param dictionary An optional file containing the complete dictionary to use
  * @param stopwords An optional file containing a list of words not to use in the dictionary
  * @param maxDF An optional maximum document frequency (as a proportion of the total number of documents) above
  *              which terms will not be used in the dictionary
  * @param minDF An optional minimum document frequency (as a proportion of the total number of documents) below
  *              which terms will not be used in the dictionary
  * @param maxFeatures An optional maximum dictionary size.
  */
case class DictionaryConfiguration (caseSensitive: Boolean,
                                    dictionary: Option[String],
                                    stopwords: Option[String],
                                    maxDF: Option[Double],
                                    minDF: Option[Double],
                                    maxFeatures: Option[Int]) {
  def needDocCount = if (minDF.isDefined || maxDF.isDefined) {
    Some(true)
  } else {
    None
  }
}

