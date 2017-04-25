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

package software.uncharted.salt.xdata.projection

import software.uncharted.salt.core.projection.numeric.MercatorProjection
import software.uncharted.salt.xdata.projection.geometry.{ArcBinner, Line, LineToPoints}
import software.uncharted.salt.xdata.projection.geometry.Line._ //scalastyle:ignore

import scala.collection.mutable.{Buffer => MutableBuffer}

/**
  * A line projection that projects straight from lines to raster bins in one pass.
  *
  * @param zoomLevels   The zoom levels onto which to project
  * @param min          The minimum coordinates of the data space
  * @param max          The maximum coordinates of the data space
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param tms          if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleLineProjection(zoomLevels: Seq[Int],
                           min: (Double, Double),
                           max: (Double, Double),
                           minLengthOpt: Option[Int] = None,
                           maxLengthOpt: Option[Int] = None,
                           tms: Boolean = true)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms) {
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates the endpoints of a line (x0, y0, x1, y1)
    * @param maxBin      The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return All the bins representing the given line if the given source row is within the bounds of the
    *         viz. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)

        val line2point = new LineToPoints(startUBin, endUBin)

        if (minLengthOpt.map(minLength => line2point.totalLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => line2point.totalLength <= maxLength).getOrElse(true)) {
          Some(line2point.rest().map { uBin => universalBinIndexToTileIndex(level, uBin, maxBin) }.toSeq)
        } else {
          None
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int)): (Int, Int) = {
    val maxX = maxBin._1 + 1
    val x = index % maxX
    (x, (index - x) / maxX)
  }
}

/**
  * A line projection that projects straight from lines to raster bins in one pass, emitting only a leader of
  * predefined length on each end of the line.
  *
  * @param zoomLevels       The zoom levels onto which to project
  * @param min              The minimum coordinates of the data space
  * @param max              The maximum coordinates of the data space
  * @param leaderLineLength The number of bins to keep near each end of the line
  * @param minLengthOpt     The minimum length of line (in bins) to project
  * @param maxLengthOpt     The maximum length of line (in bins) to project
  * @param tms              if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleLeaderLineProjection(zoomLevels: Seq[Int],
                                 min: (Double, Double),
                                 max: (Double, Double),
                                 leaderLineLength: Int = 1024, // scalastyle:ignore
                                 minLengthOpt: Option[Int] = None,
                                 maxLengthOpt: Option[Int] = None,
                                 tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms) {
  private val leaderLineLengthSquared = leaderLineLength * leaderLineLength

  // scalastyle:off method.length
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates The data-space coordinate
    * @param maxBin      The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return A series of tile coordinate/bin index pairs if the given source row is within the bounds of the viz.
    *         None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)

        val line2point = new LineToPoints(startUBin, endUBin)

        if (minLengthOpt.map(minLength => line2point.totalLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => line2point.totalLength <= maxLength).getOrElse(true)) {
          val uBins: Seq[(Int, Int)] =
            if (line2point.totalLength > 2 * leaderLineLength) {
              val startVal = line2point.longAxisValue(startUBin)
              val startLeaderEnd = {
                val skipPoint = line2point.longAxisValue(line2point.skipToDistance(startUBin, leaderLineLength))
                line2point.reset()
                var p =
                  if (line2point.increasing) {
                    line2point.skipTo(skipPoint - 1)
                  } else {
                    line2point.skipTo(skipPoint + 1)
                  }
                var pn = p
                while (Line.distanceSquared(pn, startUBin) <= leaderLineLengthSquared) {
                  p = pn
                  pn = line2point.next()
                }
                p
              }
              val endLeaderStart = {
                line2point.reset()
                var p = line2point.skipToDistance(endUBin, leaderLineLength + 1)
                while (Line.distanceSquared(p, endUBin) > leaderLineLengthSquared)
                  p = line2point.next()
                p
              }

              line2point.reset()
              val firstHalf =
                if (line2point.increasing) {
                  line2point.next(line2point.longAxisValue(startLeaderEnd) - startVal + 1)
                } else {
                  line2point.next(startVal - line2point.longAxisValue(startLeaderEnd) + 1)
                }
              line2point.reset()
              line2point.skipTo(line2point.longAxisValue(endLeaderStart) + (if (line2point.increasing) -1 else 1))
              val secondHalf = line2point.rest()

              firstHalf ++ secondHalf
            } else {
              line2point.rest()
            }

          val bins: Seq[((Int, Int, Int), (Int, Int))] = uBins.map(uBin =>
            universalBinIndexToTileIndex(level, uBin, maxBin)
          )

          Some(bins)
        } else {
          None: Option[Seq[((Int, Int, Int), (Int, Int))]]
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  // scalastyle:on method.length

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int)): (Int, Int) = {
    val maxX = maxBin._1 + 1
    val x = index % maxX
    (x, (index - x) / maxX)
  }
}





/**
  * A line projection that projects straight from arcs to raster bins in one pass.  All arcs are drawn with the same
  * curvature, and go clockwise from source to destination.
  *
  * @param zoomLevels   The zoom levels onto which to project
  * @param min          The minimum coordinates of the data space
  * @param max          The maximum coordinates of the data space
  * @param arcLength    The curvature of the arcs drawn, in radians
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param tms          if true, the Y axis for tile coordinates only is flipped     *
  */
class SimpleArcProjection(zoomLevels: Seq[Int],
                          min: (Double, Double),
                          max: (Double, Double),
                          arcLength: Double = math.Pi / 3,
                          minLengthOpt: Option[Int] = Some(4),
                          maxLengthOpt: Option[Int] = Some(1024), // scalastyle:ignore
                          tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms) {
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates the data-space coordinate
    * @param maxBin      The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return An optional sequence of points on the arc between the input coordinates, if the given input coordinates
    *         are within the bounds of the viz. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)
        val chordLength = Line.distance(startUBin, endUBin)

        if (minLengthOpt.map(minLength => chordLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => chordLength <= maxLength).getOrElse(true)) {
          // Universal Bin Coordinates have a flipped Y axis from mathematical standard, therefore we have to flip the arc direction - i.e. clockwise = false, with a flipped Y axis, generates a clockwise arc
          val arcToPoints = new ArcBinner(startUBin, endUBin, arcLength, clockwise = false)
          Some(arcToPoints.remaining.map(uBin =>
            universalBinIndexToTileIndex(level, uBin, maxBin)
          ).toSeq)
        } else {
          None
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int)): (Int, Int) = {
    val maxX = maxBin._1 + 1
    val x = index % maxX
    (x, (index - x) / maxX)
  }
}


// scalastyle:off cyclomatic.complexity
// scalastyle:off method.length
class SimpleLeaderArcProjection(zoomLevels: Seq[Int],
                                min: (Double, Double),
                                max: (Double, Double),
                                leaderLength: Int,
                                arcLength: Double = math.Pi / 3,
                                minLengthOpt: Option[Int] = Some(4),
                                maxLengthOpt: Option[Int] = Some(1024), // scalastyle:ignore
                                tms: Boolean = false)
  extends CartesianTileProjection2D[(Double, Double, Double, Double), (Int, Int)](min, max, tms) {
  /**
    * Project a data-space coordinate into the corresponding tile coordinate and bin coordinate
    *
    * @param coordinates The endpoints of an arc (x0, y0, x1, y1)
    * @param maxBin      The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return The bins near the endpoins of the given arc, if within data space bounds. None otherwise.
    */
  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    if (coordinates.isEmpty) {
      None
    } else {
      // get input points translated and scaled into [0, 1) x [0, 1)
      val startPoint = translateAndScale(coordinates.get._1, coordinates.get._2)
      val endPoint = translateAndScale(coordinates.get._3, coordinates.get._4)

      zoomLevels.map { level =>
        // Convert input into universal bin coordinates
        val startUBin = scaledToUniversalBin(startPoint, level, maxBin)
        val endUBin = scaledToUniversalBin(endPoint, level, maxBin)
        val chordLength = Line.distance(startUBin, endUBin)

        if (minLengthOpt.map(minLength => chordLength >= minLength).getOrElse(true) &&
          maxLengthOpt.map(maxLength => chordLength <= maxLength).getOrElse(true)) {
          Some {
            // Universal Bin Coordinates have a flipped Y axis from mathematical standard, therefore we have to flip the arc direction - i.e. clockwise = false, with a flipped Y axis, generates a clockwise arc
            val binner =  new ArcBinner(startUBin, endUBin, arcLength, clockwise = false)
            val center = ArcBinner.getArcCenter(startUBin, endUBin, arcLength, clockwise = false)
            val radius = ArcBinner.getArcRadius(startUBin, endUBin, arcLength)
            val leaderArcLength = ArcBinner.getArcLength(radius, leaderLength)
            if (arcLength <= 2.0 * leaderArcLength) {
              // Short arc, use the whole thing
              binner.remaining.map(uBin => universalBinIndexToTileIndex(level, uBin, maxBin)).toSeq
            } else {
              val startLeaderBuffer = MutableBuffer[(Int, Int)]()
              binner.resetToStart()
              var next = (0, 0)
              var inBounds = true
              while (binner.hasNext && inBounds) {
                next = binner.next()
                if (Line.distance(next, startUBin) < leaderLength) {
                  inBounds = true
                  startLeaderBuffer += next
                } else {
                  inBounds = false
                }
              }

              val endLeaderBuffer = MutableBuffer[(Int, Int)]()
              binner.resetToEnd()
              var previous = (0, 0)
              inBounds = true
              while (binner.hasPrevious && inBounds) {
                previous = binner.previous()
                if (Line.distance(previous, endUBin) < leaderLength) {
                  inBounds = true
                  endLeaderBuffer += previous
                } else {
                  inBounds = false
                }
              }

              (startLeaderBuffer ++ endLeaderBuffer)
                .map(uBin => universalBinIndexToTileIndex(level, uBin, maxBin))
            }
          }
        } else {
          None: Option[Seq[((Int, Int, Int), (Int, Int))]]
        }
      }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
    }
  }

  /**
    * Project a bin index BC into 1 dimension for easy storage of bin values in an array
    *
    * @param bin    A bin index
    * @param maxBin The maximum possible bin index (i.e. if your tile is 256x256, this would be (255,255))
    * @return the bin index converted into its one-dimensional representation
    */
  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int)): (Int, Int) = {
    val maxX = maxBin._1 + 1
    val x = index % maxX
    (x, (index - x) / maxX)
  }
}


object MercatorLineProjection {
  val mercatorMin = (-180.0, -85.05112878)
  val mercatorMax = (180.0, 85.05112878)
}

/**
  * A projection for lines into 2D mercator (lon,lat) space
  *
  * @param minBounds    the minimum value of a data-space coordinate (minLon, minLat)
  * @param maxBounds    the maximum value of a data-space coordinate (maxLon, maxLat)
  * @param minLengthOpt The minimum length of line (in bins) to project
  * @param maxLengthOpt The maximum length of line (in bins) to project
  * @param zoomLevels   the TMS/WMS zoom levels to project into
  */
class MercatorLineProjection(zoomLevels: Seq[Int],
                             minBounds: (Double, Double) = MercatorLineProjection.mercatorMin,
                             maxBounds: (Double, Double) = MercatorLineProjection.mercatorMax,
                             minLengthOpt: Option[Int] = None,
                             maxLengthOpt: Option[Int] = None,
                             tms: Boolean = true)
  extends MercatorTileProjection2D[(Double, Double, Double, Double), (Int, Int)](minBounds, maxBounds, tms) {

  private val mercatorProjection = new MercatorProjection(zoomLevels, minBounds, maxBounds, tms)

  override def project(coordinates: Option[(Double, Double, Double, Double)], maxBin: (Int, Int)): Option[Seq[((Int, Int, Int), (Int, Int))]] = {
    val xBins = maxBin._1 + 1
    val yBins = maxBin._2 + 1

    if (!coordinates.isDefined) {
      None
    } else {
      // compute start and end-points of the line in WMS/TMS mercator space, for each zoomLevel
      val startdc = (coordinates.get._1, coordinates.get._2)
      val enddc = (coordinates.get._3, coordinates.get._4)
      val start = mercatorProjection.project(Some(startdc), maxBin)
      val end = mercatorProjection.project(Some(enddc), maxBin)

      if (start.isDefined && end.isDefined) {
        zoomLevels.map { level =>
          // convert start and end points of line into universal bin coordinates for use in LineToPoints
          val startUniversalBin = tileBinIndexToUniversalBinIndex(start.get(level)._1, start.get(level)._2, maxBin)
          val endUniversalBin = tileBinIndexToUniversalBinIndex(end.get(level)._1, end.get(level)._2, maxBin)

          val line2point = new LineToPoints(startUniversalBin, endUniversalBin)

          if (minLengthOpt.map(minLength => line2point.totalLength >= minLength).getOrElse(true) &&
            maxLengthOpt.map(maxLength => line2point.totalLength <= maxLength).getOrElse(true)) {
            Some(line2point.rest().map { uBin => universalBinIndexToTileIndex(level, uBin, maxBin) }.toSeq)
          } else {
            None
          }
        }.reduce((a, b) => (a ++ b).reduceLeftOption(_ ++ _))
      } else {
        None
      }
    }
  }

  override def binTo1D(bin: (Int, Int), maxBin: (Int, Int)): Int = {
    bin._1 + bin._2 * (maxBin._1 + 1)
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int)): (Int, Int) = {
    val maxX = maxBin._1 + 1
    val x = index % maxX
    (x, (index - x) / maxX)
  }
}

// scalastyle:on method.length
// scalastyle:on cyclomatic.complexity
