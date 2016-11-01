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
package software.uncharted.xdata.ops.salt

import software.uncharted.salt.core.projection.numeric.{CartesianProjection, NumericProjection}

class XYTimeProjection(min: (Double, Double, Long) = (0.0, 0.0, 0),
                       max: (Double, Double, Long) = (0.0, 0.0, Long.MaxValue),
                       rangeBuckets: Long = Long.MaxValue,
                       baseProjection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)])
  extends NumericProjection[(Double, Double, Long), (Int, Int, Int), (Int, Int, Int)](min, max) {

  override def project (dCoords: Option[(Double, Double, Long)], maxBin: (Int, Int, Int)):
  Option[Seq[((Int, Int, Int), (Int, Int, Int))]] = {
    dCoords.flatMap { coords =>
      if (coords._3 >= min._3 && coords._3 <= max._3) {
        baseProjection.project(Some((coords._1, coords._2)), (maxBin._1, maxBin._2))
          .flatMap { projectedCoords =>
            Some(
              projectedCoords.map { projCoord =>
                val binSize = (max._3 - min._3) / rangeBuckets
                val timeBin = (coords._3 - min._3) / binSize
                if (timeBin < rangeBuckets) {
                  Some(projCoord._1, (projCoord._2._1, projCoord._2._2, timeBin.asInstanceOf[Int]))
                } else {
                  None
                }
              }.toSeq.flatten
            )
          }
      } else {
        None
      }
    }
  }

  override def binTo1D(bin: (Int, Int, Int), maxBin: (Int, Int, Int)): Int = {
    val result = (maxBin._1 + 1) * (maxBin._2 + 1) * bin._3 + (maxBin._1 + 1) * bin._2 + bin._1
    result
  }

  override def binFrom1D(index: Int, maxBin: (Int, Int, Int)): (Int, Int, Int) = {
    // i = xSize * ySize * z + xSize * y + x
    val xSize = maxBin._1 + 1
    val ySize = maxBin._2 + 1
    val xy = index % (xSize * ySize)
    val time = (index - xy) / (xSize * ySize)
    val x = xy % xSize
    val y = (xy - x) / xSize
    (x, y, time)
  }
}
