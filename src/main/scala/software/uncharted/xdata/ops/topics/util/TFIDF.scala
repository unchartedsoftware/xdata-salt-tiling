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

package software.uncharted.xdata.ops.topics.util

import scala.io.Source

object TFIDF extends Serializable {

  def getTfidfLocal(path: String, date: String, word_dict: scala.collection.immutable.Map[String, Int]): scala.collection.immutable.Map[Int,Double] = {
    val lines = Source.fromFile(path).getLines.map(_.split("\t"))
    lines.filter(x => x(0) == date)
      .map(x => (x(1), x(2).toDouble))
      .filter(x => word_dict contains x._1 )
      .map(x => (word_dict.get(x._1).get, x._2))
      .toMap
  }

  def loadTfidf(path: String, dates: Array[String]): Array[(String, String, Double)] = {
    val lines = Source.fromFile(path).getLines.map(_.split("\t"))
    lines.filter(x => dates contains x(0))
      .map(x => (x(0), x(1), x(2).toDouble)).toArray
  }

  def filterTfidf(tfidf: Array[(String, String, Double)], date: String, word_dict: scala.collection.immutable.Map[String, Int]): scala.collection.immutable.Map[Int,Double] = {
    val filtered = tfidf.filter{case (d, w, s) => d == date}
      .filter{case (d, w, s) => word_dict contains w }
      .map{case (d, w, s) => (word_dict.get(w).get, s) }
    filtered.toMap
  }
}
