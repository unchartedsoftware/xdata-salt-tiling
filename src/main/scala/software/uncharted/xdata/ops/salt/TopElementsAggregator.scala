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
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator

import scala.collection.mutable.{ListBuffer, PriorityQueue, HashMap}
import scala.reflect.ClassTag

/**
 * Aggregator for producing the top N common elements in a
 * collection column, along with their respective counts.
 *
 * Provide a work around for issue https://github.com/unchartedsoftware/salt/issues/112.
 * Once that's fixed, the base class can be used.
 *
 * @param elementLimit Produce up to this number of "top elements" in finish()
 * @tparam ET The element type
 */
class TopElementsAggregator2[ET: ClassTag](elementLimit: Int)
extends TopElementsAggregator[ET](elementLimit) {
  val localDefault = default()

  override def add(current: HashMap[ET, Int], next: Option[Seq[ET]]): HashMap[ET, Int] = {
    next.map { n =>
      val map = if (current.equals(localDefault)) new HashMap[ET, Int] else current
      n.foreach(t => map.put(t, map.getOrElse(t, 0) + 1))
      map
    }.getOrElse(current)
  }
}
