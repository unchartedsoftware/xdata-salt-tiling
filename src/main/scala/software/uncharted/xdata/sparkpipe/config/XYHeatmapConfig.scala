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

trait ProjectionConfig

class MercatorProjectionConfig extends ProjectionConfig

class CartesianProjectionConfig (val minX: Double, val minY: Double,
                                 val maxX: Double, val maxY: Double) extends ProjectionConfig

object ProjectionConfig {
  val PROJECTION_TYPE_KEY = "type"
  val MIN_X_KEY = "minX"
  val MIN_Y_KEY = "minY"
  val MAX_X_KEY = "maxX"
  val MAX_Y_KEY = "maxY"
  def apply (config: Config): Try[ProjectionConfig] = {
    Try {
      config.getString(PROJECTION_TYPE_KEY).toLowerCase.trim match {
        case "mercator" =>
          new MercatorProjectionConfig
        case "cartesian" =>
          val minX = config.getDouble(MIN_X_KEY)
          val minY = config.getDouble(MIN_Y_KEY)
          val maxX = config.getDouble(MAX_X_KEY)
          val maxY = config.getDouble(MAX_Y_KEY)
          new CartesianProjectionConfig(minX, minY, maxX, maxY)
      }
    }
  }
}



case class XYHeatmapConfig(xCol: String,
                           yCol: String,
                           valueCol: String,
                           projection: ProjectionConfig)
// Parse config for geoheatmap sparkpipe op
object XYHeatmapConfig {
  val CATEGORY_KEY = "xyHeatmap"
  val PROJECTION_KEY = "projection"
  val X_COLUMN_KEY = "xColumn"
  val Y_COLUMN_KEY = "yColumn"
  val VALUE_COLUMN_KEY = "valueColumn"

  def apply(config: Config): Try[XYHeatmapConfig] = {
    for (
      heatmapConfig <- Try(config.getConfig(CATEGORY_KEY));
      projectionConfig <- Try(heatmapConfig.getConfig(PROJECTION_KEY));
      projection <- ProjectionConfig(projectionConfig)
    ) yield {
      XYHeatmapConfig(
        heatmapConfig.getString(X_COLUMN_KEY),
        heatmapConfig.getString(Y_COLUMN_KEY),
        heatmapConfig.getString(VALUE_COLUMN_KEY),
        projection)
    }
  }
}
