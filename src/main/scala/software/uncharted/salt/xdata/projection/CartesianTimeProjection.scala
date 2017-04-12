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
package software.uncharted.salt.xdata.projection

import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.salt.xdata.util.RangeDescription

class CartesianTimeProjection(zoomLevels: Seq[Int],
                              min: (Double, Double, Long) = (0.0, 0.0, 0),
                              max: (Double, Double, Long) = (1.0, 1.0, Long.MaxValue),
                              rangeBuckets: Long = Long.MaxValue)
  extends XYTimeProjection(min, max, rangeBuckets, new CartesianProjection(zoomLevels, (min._1, min._2), (max._1, max._2))) {

  def this(zoomLevels: Seq[Int], min: (Double, Double), max: (Double, Double), timeRange: RangeDescription[Long]) = {
    this(zoomLevels, (min._1, min._2, timeRange.min), (max._1, max._2, timeRange.max), timeRange.count)
  }
}
