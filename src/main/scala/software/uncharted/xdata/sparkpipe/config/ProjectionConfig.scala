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

trait ProjectionConfig {
  def xyBounds: Option[(Double, Double, Double, Double)]
}

class MercatorProjectionConfig(bounds: Option[(Double, Double, Double, Double)])extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
}

class CartesianProjectionConfig (bounds: Option[(Double, Double, Double, Double)]) extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
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
