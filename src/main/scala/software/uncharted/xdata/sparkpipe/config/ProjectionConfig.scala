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

class MercatorProjectionConfig(bounds: Option[(Double, Double, Double, Double)])extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds

  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] =
    new MercatorProjection(levels)
}

class CartesianProjectionConfig (bounds: Option[(Double, Double, Double, Double)]) extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = {
    val xyBounds = bounds.get
    new CartesianProjection(levels, (xyBounds._1, xyBounds._2), (xyBounds._3, xyBounds._4))
  }
}

object ProjectionConfig {
  val projectionKey = "projection"
  val xyBoundsKey = "xyBounds"

  def apply(config: Config): Try[ProjectionConfig] = {
    Try {
      var xyBounds: Option[(Double, Double, Double, Double)] = None
      if (config.hasPath(xyBoundsKey)) {
        // scalastyle:ignore
        val xyBoundsAra = config.getDoubleList(xyBoundsKey).toArray(Array(Double.box(0.0)))
        xyBounds = Some(xyBoundsAra(0), xyBoundsAra(1), xyBoundsAra(2), xyBoundsAra(3))
      }
      if (config.hasPath(projectionKey)) {
        config.getString(projectionKey).toLowerCase.trim match {
          case "mercator" =>
            new MercatorProjectionConfig(xyBounds)
          case "cartesian" =>
            new CartesianProjectionConfig(xyBounds)
        }
      } else {
        new CartesianProjectionConfig(xyBounds)
      }
    }
  }
}
