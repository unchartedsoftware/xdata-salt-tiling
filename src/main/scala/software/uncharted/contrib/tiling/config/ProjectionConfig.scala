/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.contrib.tiling.config

import com.typesafe.config.{Config, ConfigException}
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
  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = {
    xyBounds match {
      case Some(b) =>
        new MercatorProjection(levels, (b._1, b._2), (b._3, b._4), tms = true)
      case None =>
        new MercatorProjection(levels, tms = true)
    }
  }
}

case class CartesianProjectionConfig (bounds: Option[(Double, Double, Double, Double)]) extends ProjectionConfig {
  override def xyBounds: Option[(Double, Double, Double, Double)] = bounds
  override def createProjection(levels: Seq[Int]): NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = {
    new CartesianProjection(levels, (xyBounds.get._1, xyBounds.get._2), (xyBounds.get._3, xyBounds.get._4))
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
            CartesianProjectionConfig(xyBounds.orElse(throw new ConfigException.Missing(xyBoundsKey)))
        }
      } else {
        throw new ConfigException.Missing(projectionKey)
      }
    }
  }
}
