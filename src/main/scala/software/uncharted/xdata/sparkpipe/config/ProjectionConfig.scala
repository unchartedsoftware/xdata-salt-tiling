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
import software.uncharted.salt.core.projection.numeric.{CartesianProjection, MercatorProjection, NumericProjection}

import scala.util.Try

trait ProjectionConfig {
  /** Get the overall bounds of all usable space under this projection */
  def xyBounds: Option[(Double, Double, Double, Double)]
  /** Create a projection object that conforms to this configuration */
  def createProjection (levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)]
}

case class MercatorProjectionConfig(bounds: Option[(Double, Double, Double, Double)]) extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] =
    new MercatorProjection(levels)
}

case class CartesianProjectionConfig (bounds: Option[(Double, Double, Double, Double)]) extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = {
    val xyBounds = bounds.get
    new CartesianProjection(levels, (xyBounds._1, xyBounds._2), (xyBounds._3, xyBounds._4))
  }
}

object ProjectionConfig extends ConfigParser {
  private val projectionKey = "projection"
  private val xyBoundsKey = "xyBounds"

  def parse(config: Config): Try[ProjectionConfig] = {
    Try {
      var xyBounds: Option[(Double, Double, Double, Double)] = None

      if (config.hasPath(xyBoundsKey)) {
        val xyBoundsArray = config.getDoubleList(xyBoundsKey).toArray(Array(Double.box(0.0)))
        xyBounds = Some(xyBoundsArray(0), xyBoundsArray(1), xyBoundsArray(2), xyBoundsArray(3))
      }

      if (config.hasPath(projectionKey)) {
        config.getString(projectionKey).toLowerCase.trim match {
          case "mercator" =>
            MercatorProjectionConfig(xyBounds)
          case "cartesian" =>
            CartesianProjectionConfig(xyBounds)
        }
      } else {
        CartesianProjectionConfig(xyBounds)
      }
    }
  }
}
