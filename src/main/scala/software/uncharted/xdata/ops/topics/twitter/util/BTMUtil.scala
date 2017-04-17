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


package software.uncharted.xdata.ops.topics.twitter.util

import java.text.SimpleDateFormat
import java.util.TimeZone
import grizzled.slf4j.Logging
import scala.util.Random


case class Biterm(biterm: (Int, Int), var z: Int) extends Serializable with Logging

object BTMUtil extends Serializable with Logging {

  /**
   * Receive an array of tokens (cleaned words). Remove stopwords, out-of-vocabulary words
   * Return an array of word-ids corresponding to each word
   */
  def getWordIds(tokens: Array[String], word_dict: Map[String, Int], stopwords: Set[String]) : Array[Int] = {
    tokens.filter(w => !(stopwords contains w))              // ignore words in stopwords
                        .map(word => word_dict.getOrElse(word, -1))         // get the word_id associated with word
                        .filter(x => x > -1)                                // ignore out-of-vocabulary words
                        // ToDo: should handled OOV words - save & output a list with counts?
  }

  def getBiterms(d:Array[Int]):Iterator[(Int, Int)] = {
    d.toSeq.combinations(2).map { case Seq(w1, w2) =>
      if (w1 < w2) (w1, w2) else (w2, w1)
    }
  }

  def extractBitermsFromTextRandomK(text: String, word_dict: Map[String, Int], stopwords: Set[String], k: Int) : Seq[Biterm] = {
    val tokens = TextUtil.cleanText(text).split("\\s+")
    val d = getWordIds(tokens, word_dict, stopwords)
    val b = getBiterms(d).toArray
    b.map(x => Biterm(x, Random.nextInt(k))).toSeq
  }

  /**
    * Generate a report of the given topics
    *
    * @param theta
    * @param phi
    * @param words
    * @param m
    * @param k The value of k
    * @param numWords The number of words to select
    * @return
    */
  // scalastyle:off magic.number
  def reportTopics(theta: Array[Double], phi:Array[Double], words: Array[String], m: Int, k: Int, numWords: Int = 20) : Array[(Double, Seq[Int])] = {
    val limit = Math.min(k, theta.length)
    Iterator.range(0, limit).toArray.map { z =>
      (theta(z), (0 until m).sortBy(w => -phi(w * limit + z)).take(numWords).map(words).asInstanceOf[Seq[Int]])
    }
  }
  // scalastyle:on magic.number

  /**
    * Measure Job running time
    * @param block
    * @tparam R
    * @return
    */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    info("\n\tRunning time: " + (t1 - t0) / 1000000000  + " sec\n")
    result
  }

  /**
    * Convert an input date of format `inFormat` to format `outFormat`
    */
  def dateParser(inFormat: SimpleDateFormat, outFormat: SimpleDateFormat)(input_date: String) : String = {
    outFormat.format(inFormat.parse(input_date))
  }

  /**
    * Create a dateParser function that reads and write twitter specific date formats
    *
    * @return a partially applied dateParser function
    */
  def makeTwitterDateParser() : String => String = {
    val inFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val outFmt = new SimpleDateFormat("yyyy-MM-dd")
    val tz = TimeZone.getTimeZone("GMT")
    inFmt.setTimeZone(tz)
    outFmt.setTimeZone(tz)
    dateParser(inFmt, outFmt) _
  }
}
