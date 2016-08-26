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

import com.typesafe.config.{Config, ConfigException}
import grizzled.slf4j.Logging
import software.uncharted.xdata.ops.salt.RangeDescription

// Parse config for geoheatmap sparkpipe op
case class XYTimeHeatmapConfig(xCol: String,
                               yCol: String,
                               timeCol: String,
                               timeRange: RangeDescription[Long],
                               projection: Option[String] = None)
object XYTimeHeatmapConfig extends Logging {

  val xyTimeHeatmapKey = "xyTimeHeatmap"
  val projectionKey = "projection"
  val xColumnKey = "xColumn"
  val yColumnKey = "yColumn"
  val timeColumnKey = "timeColumn"
  val timeMinKey = "min"
  val timeStepKey = "step"
  val timeCountKey =  "count"

  def apply(config: Config): Option[XYTimeHeatmapConfig] = {
    try {
      val heatmapConfig = config.getConfig(xyTimeHeatmapKey)
      Some(XYTimeHeatmapConfig(
        heatmapConfig.getString(xColumnKey),
        heatmapConfig.getString(yColumnKey),
        heatmapConfig.getString(timeColumnKey),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
        if (heatmapConfig.hasPath(projectionKey)) Some(heatmapConfig.getString(projectionKey)) else None)
      )
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [" + xyTimeHeatmapKey + "]", e)
        None
    }
  }
}
