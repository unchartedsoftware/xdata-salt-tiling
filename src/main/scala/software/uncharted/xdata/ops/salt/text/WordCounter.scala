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

package software.uncharted.xdata.ops.salt.text



import software.uncharted.salt.core.analytic.Aggregator

import scala.collection.mutable.{Map => MutableMap}



/**
  * An aggregator that can track counts of individual words
  */
object WordCounter {
  val wordSeparators = "('$|^'|'[^a-zA-Z_0-9']+|[^a-zA-Z_0-9']+'|[^a-zA-Z_0-9'])+"
}

class WordCounter extends Aggregator[String, MutableMap[String, Int], Map[String, Int]] {
  override def default(): MutableMap[String, Int] = MutableMap[String, Int]()

  override def finish(intermediate: MutableMap[String, Int]): Map[String, Int] = intermediate.toMap

  override def merge(left: MutableMap[String, Int], right: MutableMap[String, Int]): MutableMap[String, Int] = {
    right.foreach { case (term, frequency) =>
      left(term) = left.getOrElse(term, 0) + frequency
    }
    left
  }

  override def add(current: MutableMap[String, Int], next: Option[String]): MutableMap[String, Int] = {
    next.foreach { input =>
      input.split(WordCounter.wordSeparators).map(_.toLowerCase.trim).filter(!_.isEmpty).foreach(word =>
        current(word) = current.getOrElse(word, 0) + 1
      )
    }
    current
  }
}
