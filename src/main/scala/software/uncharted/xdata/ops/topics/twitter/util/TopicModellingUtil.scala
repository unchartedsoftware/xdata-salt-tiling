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
import java.util.{Date}
import grizzled.slf4j.Logging
import org.joda.time.{DateTime, Period}

// scalastyle:off public.methods.have.type parameter.number
/**
  * Functions for executing topic modelling jobs
  */
object TopicModellingUtil extends Logging {

  /**
    * Create a sequence of dates between the given endpoints with one day intervals
    *
    * @param from The date to start the range from
    * @param to The date to end the range on
    * @return A sequence of dates (of the format yyyy-MM-dd) between the given endpoints with one day intervals
    */
  // scalastyle:off magic.number
 def dateRange(from: Date, to: Date): Array[String] = {
   val s = Iterator.iterate(new DateTime(from))(_.plus(new Period().withDays(1))).takeWhile(!_.isAfter(new DateTime(to))).toArray
   s.map(datetime => datetime.toString.slice(0, 10))
 }

  /**
  * Create a sequence of dates between the given endpoints with one day intervals
  *
  * @param from The date to start the range from (yyyy-MM-dd)
  * @param to The date to end the range on (yyyy-MM-dd)
  * @return A sequence of dates (of the format yyyy-MM-dd) between the given endpoints with one day intervals
  */
  // scalastyle:off
 def dateRange(from: String, to: String): Array[String] = {
   val format = new SimpleDateFormat("yyyy-MM-dd")
   dateRange(format.parse(from), format.parse(to))
 }

  /**
    * Cast the results of BDP operation and cast them into their appropriate types
    *
    * @param parts An array of results, one for each partition/date the operation was run on
    */
  def castResults(parts: Array[Array[Any]]) : Array[(String,
                                                    Array[(Double, Seq[String])],
                                                    Array[Double],
                                                    Array[Double],
                                                    Map[Int, Int],
                                                    Int,
                                                    Double)] = {
    parts.map { p =>
      val date = p(0).toString
      val topic_dist = p(1).asInstanceOf[Array[(Double, Seq[String])]]
      val theta = p(2).asInstanceOf[Array[Double]]
      val phi = p(3).asInstanceOf[Array[Double]]
      val nzMap = p(4).asInstanceOf[collection.immutable.Map[Int,Int]]
      val m = p(5).asInstanceOf[Int]
      val duration = p(6).asInstanceOf[Double]
      (date, topic_dist, theta, phi, nzMap, m, duration)
    }
  }

  /**
    * Extract top 3 hashtags from each topic
    *
    * @param tp a Row of topics
    *
    */
  def findLabels(tp: Seq[String]): Seq[String] = {
    val hashtags = tp.filter(_.startsWith("#")).take(3)
    val terms = tp.filterNot(_.startsWith("#")).take(3)
    val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take 3
    labels
  }
}
