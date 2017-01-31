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
package software.uncharted.xdata.sparkpipe.config

import com.typesafe.config.Config

import scala.util.Try
// scalastyle:ignore
case class XYHeatmapConfig(xCol: String,
                           yCol: String,
                           valueCol: String,
                           projection: Option[String] = None,
                           xyBounds: Option[(Double, Double, Double, Double)] = None)

// Parse config for geoheatmap sparkpipe op
object XYHeatmapConfig {
  val xyHeatmapKey = "xyHeatmap"
  val projectionKey = "projection"
  val xColumnKey = "xColumn"
  val yColumnKey = "yColumn"
  val valueColumnKey = "valueColumn"
  val xyBoundsKey = "xyBounds"

  def apply(config: Config): Try[XYHeatmapConfig] = {
    Try {
      val heatmapConfig = config.getConfig(xyHeatmapKey)
      XYHeatmapConfig(
        heatmapConfig.getString(xColumnKey),
        heatmapConfig.getString(yColumnKey),
        heatmapConfig.getString(valueColumnKey),
        if (heatmapConfig.hasPath(projectionKey)) Some(heatmapConfig.getString(projectionKey)) else None,
        if (heatmapConfig.hasPath(xyBoundsKey)) { // scalastyle:ignore
          val xyBounds = heatmapConfig.getDoubleList(xyBoundsKey).toArray(Array(Double.box(0.0)))
          Some((xyBounds(0), xyBounds(1), xyBounds(2), xyBounds(3)))
        } else None )
    }
  }
}
