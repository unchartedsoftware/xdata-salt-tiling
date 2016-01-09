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
package software.uncharted.xdata.ops.salt

import software.uncharted.salt.core.analytic.Aggregator

import scala.collection.immutable.Map
import scala.reflect.ClassTag

class ElementScoreAggregator[ET: ClassTag](limit: Int)
  extends Aggregator[Seq[(ET, Int)], Map[ET, Int], List[(ET, Int)]] {

  override def default(): Map[ET, Int] = Map()

  override def finish(intermediate: Map[ET, Int]): List[(ET, Int)] = intermediate.toList.sortBy(e => -e._2).take(limit)

  override def merge(left: Map[ET, Int], right: Map[ET, Int]): Map[ET, Int] = {
    left.foldLeft(right) { (merged, leftEntry) =>
      merged + (leftEntry._1 -> (merged.getOrElse(leftEntry._1, 0) + leftEntry._2))
    }
  }

  override def add(current: Map[ET, Int], next: Option[Seq[(ET, Int)]]): Map[ET, Int] = {
    next.map { s =>
      s.foldLeft(current) { (counts, elementCount) =>
        counts + (elementCount._1 -> (counts.getOrElse(elementCount._1, 0) + elementCount._2))
      }
    }.getOrElse(current)
  }
}
