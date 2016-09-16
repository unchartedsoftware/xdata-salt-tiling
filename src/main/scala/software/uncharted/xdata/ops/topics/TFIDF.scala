/*
 * Copyright © 2013-2015 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
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
 *
 * Created by chagerman on 2016-04-27.
 */

package com.uncharted.btm

import scala.io.Source

// ToDo - add in / incorporate the code which actually computes TFIDF both as a batch and incrementally
// ----------------------------------------------------------------------------------------------------------------------------------
object TFIDF extends Serializable {

//  def getTfidf_rdd(path: String, date: String, word_dict:  Map[String, Int]): Map[Int,Double] = {
//    val tfidf_rdd = sc.textFile(path).map(_.split("\t"))
//    // val tmp = Source.fromFile(path).getLines.map(_.split("\t")).toArray
//    val tfidf_dict: Map[Int, Double] = tfidf_rdd.filter(x => x(0) == date)
//      .map(x => (x(1), x(2).toDouble))
//      .filter(x => word_dict contains x._1 )
//      .map(x => (word_dict.get(x._1).get, x._2))
//      .collect
//      .toMap
//    tfidf_dict
//  }





  def getTfidf(path: String, date: String, word_dict: scala.collection.immutable.Map[String, Int]): scala.collection.immutable.Map[Int,Double] = {
    val lines = Source.fromFile(path).getLines.map(_.split("\t"))
    val tfidf_dict = lines.filter(x => x(0) == date)
      .map(x => (x(1), x(2).toDouble))
      .filter(x => word_dict contains x._1 )
      .map(x => (word_dict.get(x._1).get, x._2))
      .toMap
    tfidf_dict
  }

}




