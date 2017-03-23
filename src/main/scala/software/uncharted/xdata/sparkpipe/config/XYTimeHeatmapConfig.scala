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
import software.uncharted.xdata.ops.salt.RangeDescription

import scala.util.Try

// Parse config for geoheatmap sparkpipe op
case class XYTimeHeatmapConfig(xCol: String,
                               yCol: String,
                               timeCol: String,
                               timeRange: RangeDescription[Long],
                               projection: ProjectionConfig)
object XYTimeHeatmapConfig extends ConfigParser {

  private val xyTimeHeatmap = "xyTimeHeatmap"
  private val xColumn = "xColumn"
  private val yColumn = "yColumn"
  private val timeColumn = "timeColumn"
  private val timeMin = "min"
  private val timeStep = "step"
  private val timeCount =  "count"

  def parse(config: Config): Try[XYTimeHeatmapConfig] = {
    for (
      heatmapConfig <- Try(config.getConfig(xyTimeHeatmap));
      projection <- ProjectionConfig.parse(heatmapConfig)
    ) yield {
      XYTimeHeatmapConfig(
        heatmapConfig.getString(xColumn),
        heatmapConfig.getString(yColumn),
        heatmapConfig.getString(timeColumn),
        RangeDescription.fromMin(heatmapConfig.getLong(timeMin), heatmapConfig.getLong(timeStep), heatmapConfig.getInt(timeCount)),
        projection
      )
    }
  }
}
