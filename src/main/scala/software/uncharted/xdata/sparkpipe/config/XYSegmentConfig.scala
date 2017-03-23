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
                           minSegLen: Option[Int] = None,
                           maxSegLen: Option[Int] = None,
                           x1Col: String,
                           y1Col: String,
                           x2Col: String,
                           y2Col: String,
                           tileSize: Int,
                           projectionConfig: ProjectionConfig)

object XYSegmentConfig extends ConfigParser {
  private val xySegment = "xySegment"
  private val arcTypeKey = "arcType"
  private val minSegLen = "minSegLen"
  private val maxSegLen = "maxSegLen"
  private val x1Col = "x1Column"
  private val y1Col = "y1Column"
  private val x2Col = "x2Column"
  private val y2Col= "y2Column"
  private val tileSize = "tileSize"

  def parse(config: Config): Try[XYSegmentConfig] = {
    for (
      segmentConfig <- Try(config.getConfig(xySegment));
      projection <- ProjectionConfig.parse(segmentConfig)
    ) yield {
      val arcType: ArcTypes.Value = segmentConfig.getString(arcTypeKey).toLowerCase match {
        case "fullline" => ArcTypes.FullLine
        case "leaderline" => ArcTypes.LeaderLine
        case "fullarc" => ArcTypes.FullArc
        case "leaderarc" => ArcTypes.LeaderArc
      }
      XYSegmentConfig(
        arcType,
        if (segmentConfig.hasPath(minSegLen)) Some(segmentConfig.getInt(minSegLen)) else None,
        if (segmentConfig.hasPath(maxSegLen)) Some(segmentConfig.getInt(maxSegLen)) else None,
        segmentConfig.getString(x1Col),
        segmentConfig.getString(y1Col),
        segmentConfig.getString(x2Col),
        segmentConfig.getString(y2Col),
        segmentConfig.getInt(tileSize),
        projection
      )
    }
  }
}
