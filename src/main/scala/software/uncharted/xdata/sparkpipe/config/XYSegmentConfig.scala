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
import software.uncharted.xdata.ops.salt.{ArcTypes, RangeDescription}
import software.uncharted.xdata.sparkpipe.config

// TODO

// Parse config for geoheatmap sparkpipe op
 case class XYSegmentConfig()

object XYSegmentConfig extends Logging {

  // val xyTimeHeatmapKey = "xyTimeHeatmap"
  // val projectionKey = "projection"
  // val xColumnKey = "xColumn"
  // val yColumnKey = "yColumn"
  // val timeColumnKey = "timeColumn"
  // val timeMinKey = "min"
  // val timeStepKey = "step"
  // val timeCountKey =  "count"

  val arcTypeKey = "arcType"
  val minSegLenKey = "minSegLen" // Option[Int],
  val maxSegLenKey = "maxSegLen" // Option[Int],
  val x1ColKey = "x1Col" // String,
  val y1ColKey = "y1Col" // String,
  val x2ColKey = "x2Col" // String,
  val y2ColKey = "y2Col" // String,
  val xyBoundsKey = "xyBounds" // (Double, Double, Double, Double),
  val zBoundsKey = "zBounds" // (Int, Int),
  val valueExtractorKey = "valueExtractor" // Row => Option[T],
  val binAggregatorKey = "binAggregator" // Aggregator[T, U, V],
  val tileAggregatorKey = "tileAggregator" // Option[Aggregator[V, W, X]],
  val tileSizeKey = "tileSize" // Int)


  def apply(config: Config): Option[XYSegmentConfig] = {
    try {
      // ArcTypes: string => FullLine, LeaderLine, FullArc, LeaderArc
      val arcType: ArcTypes = config.getString(arcTypeKey).toLower match {
        case "fullline" => ArcTypes.FullLine
        case "leaderline" => ArcTypes.LeaderLine
        case "fullarc" => ArcTypes.FullArc
        case "leaderarc" => ArcTypes.LeaderArc
      }

      val minSegLen = if config.hasPath(minSegLenKey) Some(config.getInt(minSegLenKey)) else None
      val maxSegLen = if config.hasPath(maxSegLenKey) Some(config.getInt(maxSegLenKey)) else None
      val x1Col = config.getString(x1ColKey)
      val y1Col = config.getString(y1ColKey)
      val x2Col = config.getString(x2ColKey)
      val y2Col = config.getString(y2ColKey)
      val xyBounds = config.getDoubleList(xyBoundsKey)
      val zBounds = config.getConfig(zBoundsKey)
      // val valueExtractor = config.getConfig(valueExtractorKey)
      // val binAggregator = config.getConfig(binAggregatorKey)
      // val tileAggregator = config.getConfig(tileAggregatorKey)
      val tileSize = config.getInt(tileSizeKey)

      // Some(XYSegmentConfig(
      //   heatmapConfig.getString(xColumnKey),
      //   heatmapConfig.getString(yColumnKey),
      //   heatmapConfig.getString(timeColumnKey),
      //   RangeDescription.fromMin(heatmapConfig.getLong(timeMinKey), heatmapConfig.getLong(timeStepKey), heatmapConfig.getInt(timeCountKey)),
      //   if (heatmapConfig.hasPath(projectionKey)) Some(heatmapConfig.getString(projectionKey)) else None)
      // )
      Some XYSegmentConfig()
    } catch {
      case e: ConfigException =>
        // error("Failure parsing arguments from [" + xyTimeHeatmapKey + "]", e)
        None
    }
  }
}
