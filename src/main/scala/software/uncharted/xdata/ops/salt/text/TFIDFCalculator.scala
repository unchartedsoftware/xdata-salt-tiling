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




sealed trait TFType {
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
  * Term frequency, semi-normalized to a logarithmic scale (i.e., not really normalized, but the logarithmic scale
  * shrinks the range enough so as to put everything into a comprehensible range)
  */
object LogNormalizedTF extends TFType {
  override def termFrequency(rawF: Int, nTerms: Int, maxRawF: Int): Double = {
    1.0 + math.log(rawF)
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



sealed trait IDFType {
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
  * Not sure exactly what this does
  */
object ProbablisticIDF extends IDFType {
  override def inverseDocumentFrequency(N: Long, n_t: Int): Double = {
    math.log((N.toDouble - n_t.toDouble) / n_t.toDouble)
  }
}


object TFIDFCalculatorParser extends ConfigParser {
  private val SECTION_KEY = "tf-idf"
  private val TYPE_KEY = "type"

  private val TF_SECTION_KEY_1 = "term-frequency"
  private val TF_SECTION_KEY_2 = "tf"
  private val TF_TYPE_BINARY = "binary"
  private val TF_TYPE_RAW = "raw"
  private val TF_TYPE_NORMALIZED = "normalized"
  private val TF_TYPE_LOG = "log"
  private val TF_TYPE_DOUBLE_NORMALIZED = "double-normalized"
  private val TF_DOUBLE_NORMALIZED_K_KEY = "k"

  private val DEFAULT_TF_TYPE_NAME = TF_TYPE_LOG
  private val DEFAULT_TF_TYPE = LogNormalizedTF
  private def tfConfig (config: Config): Try[TFType] = {
    Try {
      getConfigOption(config, SECTION_KEY).map { tfIdfSection =>
        getConfigOption(config, TF_SECTION_KEY_1, TF_SECTION_KEY_2).map { tfSection =>
          getString(tfSection, TYPE_KEY, DEFAULT_TF_TYPE_NAME) match {
            case TF_TYPE_BINARY => BinaryTF.asInstanceOf
            case TF_TYPE_RAW => RawTF
            case TF_TYPE_NORMALIZED => DocumentNormalizedTF
            case TF_TYPE_LOG => LogNormalizedTF
            case TF_TYPE_DOUBLE_NORMALIZED =>
              val k = getDoubleOption(tfSection, TF_DOUBLE_NORMALIZED_K_KEY).get
              TermNormalizedTF(k)
          }
        }.getOrElse(DEFAULT_TF_TYPE)
      }.getOrElse(DEFAULT_TF_TYPE)
    }
  }

  private val IDF_SECTION_KEY_1 = "inverse-document-frequency"
  private val IDF_SECTION_KEY_2 = "idf"
  private val IDF_TYPES = Map(
    "unary"         -> UnaryIDF,
    "linear"        -> LinearIDF,
    "log"           -> LogIDF,
    "log-smooth"    -> SmoothLogIDF,
    "probabalistic" -> ProbablisticIDF
  )
  private val DEFAULT_IDF_TYPE_NAME = "log-smooth"
  private def idfConfig (config: Config): IDFType = {
    getConfigOption(config, SECTION_KEY).map { tfIdfSection =>
      getConfigOption(config, IDF_SECTION_KEY_1, IDF_SECTION_KEY_2).flatMap { section =>
        IDF_TYPES.get(getString(section, TYPE_KEY, DEFAULT_IDF_TYPE_NAME))
      }
    }.getOrElse(IDF_TYPES(DEFAULT_IDF_TYPE_NAME))
  }

  def tfIdfConfig (config: Config): Try[(TFType, IDFType)] = {
    tfConfig(config).map(tf =>
      (tf, idfConfig(config))
    )
  }
}
