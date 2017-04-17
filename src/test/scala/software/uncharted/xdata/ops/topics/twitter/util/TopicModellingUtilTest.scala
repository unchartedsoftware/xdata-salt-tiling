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

import java.util.{Calendar, Date, GregorianCalendar}

import org.scalatest.FunSpec

class TopicModellingUtilTest extends FunSpec {
  describe("#TopicModellingUtilTest") {
    it ("should test dateRange which takes in Java Date inputs") {
      val c = Calendar.getInstance()
      c.set(2017, 2, 17)
      val startDate = c.getTime
      c.set(2017, 2, 20)
      val endDate = c.getTime

      val result = TopicModellingUtil.dateRange(startDate, endDate)
      val expected = Array("2017-03-17", "2017-03-18", "2017-03-19", "2017-03-20")

      assertResult(expected)(result)
    }

    it ("should test dateRange which takes in string date inputs") {
      val startDate = "2017-03-17"
      val endDate = "2017-03-20"
      val result = TopicModellingUtil.dateRange(startDate, endDate)
      val expected = Array("2017-03-17", "2017-03-18", "2017-03-19", "2017-03-20")
      assertResult(expected)(result)
    }

    it ("should cast results from BDP operation to appropriate types") {
      val c = Calendar.getInstance
      c.set(2017, 2, 17)
      val date = c.getTime
      val topic_dist = Array((0.18, Vector("hello", "world")), (0.21, Vector("music", "countdown")))
      val theta = Array(1.0, 2.0, 5.0)
      val phi = Array(1.3E-4, 1.2E-4, 1.98E-5)
      val nzMap = Map(0->10, 5->7, 2->7)
      val m = 2
      val duration = 1.12E-5
      val parts = Array(Array(date, topic_dist, theta, phi, nzMap, m, duration))
      val result = TopicModellingUtil.castResults(parts)
      val expected = Array((date.toString, topic_dist, theta, phi, nzMap, m, duration))

      assertResult(expected)(result)
    }

    it ("should extract the top 3 hashtags from each topic") {
      val topicRows1 = Seq("#videogames", "flowers", "caturday", "music", "bestday", "#countdown")
      val topicRows2 = Seq("#videogames", "#flowers", "#caturday", "music", "bestday", "#countdown")
      val topicRows3 = Seq("videogames", "flowers", "caturday", "music", "bestday", "countdown")

      val result1 = TopicModellingUtil.findLabels(topicRows1)
      val result2 = TopicModellingUtil.findLabels(topicRows2)
      val result3 = TopicModellingUtil.findLabels(topicRows3)

      val expected1 = Seq("#videogames", "#countdown", "flowers")
      val expected2= Seq("#videogames", "#flowers", "#caturday")
      val expected3= Seq("videogames", "flowers", "caturday")

      assertResult(expected1)(result1) //combines hashtags with terms
      assertResult(expected2)(result2)
      assertResult(expected3)(result3)
    }
  }

}
