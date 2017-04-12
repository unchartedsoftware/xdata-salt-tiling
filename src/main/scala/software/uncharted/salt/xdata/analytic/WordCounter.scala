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
package software.uncharted.salt.xdata.analytic

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
