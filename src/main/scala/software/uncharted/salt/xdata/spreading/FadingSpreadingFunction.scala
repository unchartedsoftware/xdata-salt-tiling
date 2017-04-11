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

package software.uncharted.salt.xdata.spreading

import software.uncharted.salt.core.spreading.SpreadingFunction
import software.uncharted.salt.xdata.projection.CartesianBinning

/**
  * Fade the ends of a line (as produced by the SimpleLeaderLineProjection) so they are bright near the endpoints,
  * and dull near the middle.
  */
class FadingSpreadingFunction(leaderLineLength: Int, maxBin: (Int, Int), _tms: Boolean)
  extends SpreadingFunction[(Int, Int, Int), (Int, Int), Double]
    with CartesianBinning {
  override protected def tms: Boolean = _tms

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value             the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  override def spread(coordsTraversable: Traversable[((Int, Int, Int), (Int, Int))],
                      value: Option[Double]): Traversable[((Int, Int, Int), (Int, Int), Option[Double])] = {
    val coords = coordsTraversable.toSeq
    val n = coords.length
    val halfWay = n / 2

    val all =
      if (n < 2 * leaderLineLength) {
        true
      } else {
        // Figure out if the midpoints are more than one bin apart
        val midBinLeft = coords(halfWay - 1)
        val midBinRight = coords(halfWay)
        val midUBinLeft = tileBinIndexToUniversalBinIndex(midBinLeft._1, midBinLeft._2, maxBin)
        val midUBinRight = tileBinIndexToUniversalBinIndex(midBinRight._1, midBinRight._2, maxBin)
        (midUBinLeft._1 - midUBinRight._1).abs < 2 && (midUBinLeft._2 - midUBinRight._2).abs < 2
      }

    if (all) {
      // No gaps, so just use all points without scaling
      coords.map { case (tile, bin) => (tile, bin, value) }
    } else {
      // Gap in the middle; scale points according to their distance from the end
      var i = 0
      coords.map { case (tile, bin) =>
        val scale =
          if (i < halfWay) {
            (halfWay - i).toDouble / halfWay
          } else {
            (i - halfWay + 1).toDouble / halfWay
          }
        i += 1

        (tile, bin, value.map(v => v * scale))
      }
    }
  }
}
