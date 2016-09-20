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

package software.uncharted.xdata.ops.topics

import org.apache.spark.Partitioner

class DatePartitioner (dates: Array[String]) extends Partitioner {
  val dateMap = dates.zipWithIndex.toMap

  override def numPartitions: Int = {
    val num_parts = dates.length
    num_parts
  }
  override def getPartition(key: Any): Int =  {
    val part = dateMap.get(key.toString).get
    part
  }
}
