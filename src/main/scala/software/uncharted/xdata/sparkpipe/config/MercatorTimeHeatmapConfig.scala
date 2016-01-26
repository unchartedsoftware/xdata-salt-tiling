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
case class MercatorTimeHeatmapConfig(lonCol: String, latCol: String, timeCol: String, timeRange: RangeDescription[Long], timeFormat: Option[String] = None)
object MercatorTimeHeatmapConfig extends Logging {

  val mercatorTimeHeatmapKey = "mercatorTimeHeatmap"
  val timeFormatKey = "timeFormat"
  val longitudeColumnKey = "longitudeColumn"
  val latitudeColumnKey = "latitudeColumn"
  val timeColumnKey = "timeColumn"
  val timeMinKey = "min"
  val timeStepKey = "step"
  val timeCountKey =  "count"

  def apply(config: Config): Option[MercatorTimeHeatmapConfig] = {
    try {
      val heatmapConfig = config.getConfig(mercatorTimeHeatmapKey)
      Some(MercatorTimeHeatmapConfig(
        heatmapConfig.getString(longitudeColumnKey),
        heatmapConfig.getString(latitudeColumnKey),
        heatmapConfig.getString(timeColumnKey),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
        if (heatmapConfig.hasPath(timeFormatKey)) Some(heatmapConfig.getString(timeFormatKey)) else None)
      )
    } catch {
      case e: ConfigException =>
        error("Failure parsing arguments from [" + mercatorTimeHeatmapKey + "]", e)
        None
    }
  }
}
