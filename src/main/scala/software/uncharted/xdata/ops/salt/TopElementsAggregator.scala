/*
 * Copyright 2016 Uncharted Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
