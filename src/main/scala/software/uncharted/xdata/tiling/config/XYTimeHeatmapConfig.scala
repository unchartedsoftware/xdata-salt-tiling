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
package software.uncharted.xdata.tiling.config

import com.typesafe.config.Config
import software.uncharted.salt.xdata.util.RangeDescription

import scala.util.Try

// Parse config for geoheatmap sparkpipe op
case class XYTimeHeatmapConfig(xCol: String,
                               yCol: String,
                               timeCol: String,
                               timeRange: RangeDescription[Long],
                               projection: ProjectionConfig)
object XYTimeHeatmapConfig extends ConfigParser {

  private val xyTimeHeatmapKey = "xyTimeHeatmap"
  private val xColumnKey = "xColumn"
  private val yColumnKey = "yColumn"
  private val timeColumnKey = "timeColumn"
  private val timeMinKey = "min"
  private val timeStepKey = "step"
  private val timeCountKey =  "count"

  def parse(config: Config): Try[XYTimeHeatmapConfig] = {
    for (
      heatmapConfig <- Try(config.getConfig(xyTimeHeatmapKey));
      projection <- ProjectionConfig.parse(heatmapConfig)
    ) yield {
      XYTimeHeatmapConfig(
        heatmapConfig.getString(xColumnKey),
        heatmapConfig.getString(yColumnKey),
        heatmapConfig.getString(timeColumnKey),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
        projection
      )
    }
  }
}
