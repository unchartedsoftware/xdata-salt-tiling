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
import software.uncharted.xdata.ops.salt.ArcTypes

import scala.util.Try

// Parse config for segment sparkpipe op
case class XYSegmentConfig(arcType: ArcTypes.Value,
                           projection: Option[String] = None,
                           minSegLen: Option[Int] = None,
                           maxSegLen: Option[Int] = None,
                           x1Col: String,
                           y1Col: String,
                           x2Col: String,
                           y2Col: String,
                           xyBounds: (Double, Double, Double, Double),
                           tileSize: Int)

object XYSegmentConfig {
  val xySegmentKey = "xySegment"
  val arcTypeKey = "arcType"
  val projectionKey = "projection"
  val minSegLenKey = "minSegLen"
  val maxSegLenKey = "maxSegLen"
  val x1ColKey = "x1Column"
  val y1ColKey = "y1Column"
  val x2ColKey = "x2Column"
  val y2ColKey = "y2Column"
  val xyBoundsKey = "xyBounds"
  val tileSizeKey = "tileSize"

  def apply(config: Config): Try[XYSegmentConfig] = {
    Try{
      val segmentConfig = config.getConfig(xySegmentKey)
      val arcType: ArcTypes.Value = segmentConfig.getString(arcTypeKey).toLowerCase match {
        case "fullline" => ArcTypes.FullLine
        case "leaderline" => ArcTypes.LeaderLine
        case "fullarc" => ArcTypes.FullArc
        case "leaderarc" => ArcTypes.LeaderArc
      }
      val xyBounds = segmentConfig.getDoubleList(xyBoundsKey).toArray(Array(Double.box(0.0)))
      XYSegmentConfig(
        arcType,
        if (segmentConfig.hasPath(projectionKey)) Some(segmentConfig.getString(projectionKey)) else None,
        if (segmentConfig.hasPath(minSegLenKey)) Some(segmentConfig.getInt(minSegLenKey)) else None,
        if (segmentConfig.hasPath(maxSegLenKey)) Some(segmentConfig.getInt(maxSegLenKey)) else None,
        segmentConfig.getString(x1ColKey),
        segmentConfig.getString(y1ColKey),
        segmentConfig.getString(x2ColKey),
        segmentConfig.getString(y2ColKey),
        (xyBounds(0), xyBounds(1), xyBounds(2), xyBounds(3)),
        segmentConfig.getInt(tileSizeKey)
      )
    }
  }
}
