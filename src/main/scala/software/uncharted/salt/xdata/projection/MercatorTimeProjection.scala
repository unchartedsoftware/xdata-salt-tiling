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

import software.uncharted.salt.core.projection.numeric.MercatorProjection
import software.uncharted.salt.xdata.util.RangeDescription

object MercatorTimeProjection  {
  val minLon = -180.0
  val maxLon = 180.0
  val minLat = -85.05112878
  val maxLat = 85.05112878
}

class MercatorTimeProjection(zoomLevels: Seq[Int],
                             min: (Double, Double, Long) = (MercatorTimeProjection.minLon, MercatorTimeProjection.minLat, 0),
                             max: (Double, Double, Long) = (MercatorTimeProjection.maxLon, MercatorTimeProjection.maxLat, Long.MaxValue),
                             rangeBuckets: Long = Long.MaxValue,
                             tms: Boolean = true)
  extends XYTimeProjection(min, max, rangeBuckets, new MercatorProjection(zoomLevels, (min._1, min._2), (max._1, max._2), tms)) {

  def this(zoomLevels: Seq[Int], timeRange: RangeDescription[Long]) = {
    this(zoomLevels, (MercatorTimeProjection.minLon, MercatorTimeProjection.minLat, timeRange.min),
      (MercatorTimeProjection.maxLon, MercatorTimeProjection.maxLat, timeRange.max),
    timeRange.count)
  }
}
