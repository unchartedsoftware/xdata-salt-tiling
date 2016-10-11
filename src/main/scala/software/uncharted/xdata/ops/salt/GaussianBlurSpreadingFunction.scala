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

import software.uncharted.salt.core.spreading.SpreadingFunction
import software.uncharted.xdata.ops.salt.GaussianBlurSpreadingFunction.{Bin2DCoord, Bin3DCoord, TileCoord}

abstract class GaussianBlurSpreadingFunction[BC](radius: Int, sigma: Double)
  extends SpreadingFunction[TileCoord, BC, Double] {

  val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
  val kernelDimension = GaussianBlurSpreadingFunction.calcKernelDimension(radius)

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value             the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  def spread(coordsTraversable: Traversable[(TileCoord, BC)], value: Option[Double]): Traversable[(TileCoord, BC, Option[Double])] = {
    val coordsValueMap = coordsTraversable
      .flatMap(addNeighbouringBins(value.getOrElse(1.0))) // Add bins in neighborhood, affected by gaussian blur
      .groupBy(coordsValueMap => coordsValueMap._1) // Group by key. Key: (tileCoordinate, BinCoordinate2D)
      .map({ case (group, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }) // Reduces by key, adding the values

    // TODO: Can I think of a better way to write this
    coordsValueMap.map(applyKernel(coordsValueMap))
  }

  def addNeighbouringBins(value: Double)
                         (coords: (TileCoord, BC)): Map[(TileCoord, BC), Double] = {
    val tileCoordinate = coords._1
    val binCoordinate = coords._2
    var result = Map(((tileCoordinate, binCoordinate) -> value))

    // Translate kernel coordinates into tile and bin coordinates and add them to result
    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, binCoordinate, (kernelX, kernelY))
        if (!result.contains(kernelTileCoord, kernelBinCoord)) {
          result = result + ((kernelTileCoord, kernelBinCoord) -> 0.0)
        }
      }
    }

    result
  }

  def applyKernel(coordsValueMapTraversable: Map[(TileCoord, BC), Double])
                 (coordsValueMap: ((TileCoord, BC), Double)): (TileCoord, BC, Option[Double]) = {
    val coords = coordsValueMap._1
    val data = coordsValueMap._2
    val tileCoordinate = coords._1
    val binCoordinate = coords._2
    var result = List[Double]()

    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, binCoordinate, (kernelX, kernelY))
        val coordValue = coordsValueMapTraversable.get((kernelTileCoord, kernelBinCoord))
        result = result :+ kernel(kernelX)(kernelY) * coordValue.getOrElse(0.0) // If no value found, then coordinate isn't part of the original coordsTraversable, so assume value is 0.
      }
    }

    (tileCoordinate, binCoordinate, Some(result.sum))
  }

  def calcKernelCoord(tileCoord: TileCoord, binCoordinate: BC, kernelIndex: Bin2DCoord): (TileCoord, BC)

  // TODO: Do I need to take into consideration tms here?
  def translateLeft(tileCoordinate: TileCoord) = (tileCoordinate._1 - 1, tileCoordinate._2, tileCoordinate._3)

  def translateRight(tileCoordinate: TileCoord) = (tileCoordinate._1 + 1, tileCoordinate._2, tileCoordinate._3)

  def translateUp(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 + 1, tileCoordinate._3)

  def translateDown(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 - 1, tileCoordinate._3)
}

class GaussianBlurSpreadingFunction2D(radius: Int, sigma: Double, maxBins: Bin2DCoord)
  extends GaussianBlurSpreadingFunction[Bin2DCoord](radius: Int, sigma: Double) {

  def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin2DCoord, kernelIndex: Bin2DCoord): (TileCoord, Bin2DCoord) = {
    var kernelXCoord = binCoord._1 + kernelIndex._1 - Math.floor(kernelDimension / 2).toInt
    var kernelYCoord = binCoord._2 + kernelIndex._2 - Math.floor(kernelDimension / 2).toInt
    var kernelBinCoord = (kernelXCoord, kernelYCoord)
    var kernelTileCoord = tileCoord

    // If kernel bin coordinate lies outside of the tile, calculate new coordinates for tile and bin
    if (kernelBinCoord._1 < 0) {
      kernelTileCoord = translateLeft(kernelTileCoord)
      kernelBinCoord = calcBinCoordInLeftTile(kernelBinCoord)
    } else if (kernelBinCoord._1 > maxBins._1) {
      kernelTileCoord = translateRight(kernelTileCoord)
      kernelBinCoord = calcBinCoordInRightTile(kernelBinCoord)
    }

    if (kernelBinCoord._2 < 0) {
      kernelTileCoord = translateUp(kernelTileCoord)
      kernelBinCoord = calcBinCoordInTopTile(kernelBinCoord)
    } else if (kernelBinCoord._2 > maxBins._2) {
      kernelTileCoord = translateDown(kernelTileCoord)
      kernelBinCoord = calcBinCoordInBottomTile(kernelBinCoord)
    }

    (kernelTileCoord, kernelBinCoord)
  }

  def calcBinCoordInLeftTile(kernelBinCoord: Bin2DCoord) = (maxBins._1 + kernelBinCoord._1 + 1, kernelBinCoord._2)

  def calcBinCoordInRightTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1 - maxBins._1 - 1, kernelBinCoord._2)

  def calcBinCoordInTopTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1, maxBins._2 + kernelBinCoord._2 + 1)

  def calcBinCoordInBottomTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1, kernelBinCoord._2 - maxBins._2 - 1)
}

class GaussianBlurSpreadingFunction3D(radius: Int, sigma: Double, maxBins: Bin3DCoord)
  extends GaussianBlurSpreadingFunction[Bin3DCoord](radius: Int, sigma: Double) {

  def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin3DCoord, kernelIndex: Bin2DCoord): (TileCoord, Bin3DCoord) = {
    var kernelBinCoord = (binCoord._1 + kernelIndex._1 - Math.floor(kernelDimension / 2).toInt, binCoord._2 + kernelIndex._2 - Math.floor(kernelDimension / 2).toInt, binCoord._3)
    var kernelTileCoord = tileCoord

    // If kernel bin coordinate lies outside of the tile, calculate new coordinates for tile and bin
    if (kernelBinCoord._1 < 0) {
      kernelTileCoord = translateLeft(kernelTileCoord)
      kernelBinCoord = calcBinCoordInLeftTile(kernelBinCoord)
    } else if (kernelBinCoord._1 > maxBins._1) {
      kernelTileCoord = translateRight(kernelTileCoord)
      kernelBinCoord = calcBinCoordInRightTile(kernelBinCoord)
    }

    if (kernelBinCoord._2 < 0) {
      kernelTileCoord = translateUp(kernelTileCoord)
      kernelBinCoord = calcBinCoordInTopTile(kernelBinCoord)
    } else if (kernelBinCoord._2 > maxBins._2) {
      kernelTileCoord = translateDown(kernelTileCoord)
      kernelBinCoord = calcBinCoordInBottomTile(kernelBinCoord)
    }

    (kernelTileCoord, kernelBinCoord)
  }

  def calcBinCoordInLeftTile(kernelBinCoord: Bin3DCoord) = (maxBins._1 + kernelBinCoord._1 + 1, kernelBinCoord._2, kernelBinCoord._3)

  def calcBinCoordInRightTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1 - maxBins._1 - 1, kernelBinCoord._2, kernelBinCoord._3)

  def calcBinCoordInTopTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1, maxBins._2 + kernelBinCoord._2 + 1, kernelBinCoord._3)

  def calcBinCoordInBottomTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1, kernelBinCoord._2 - maxBins._2 - 1, kernelBinCoord._3)
}

object GaussianBlurSpreadingFunction {
  type TileCoord = (Int, Int, Int)
  type Bin2DCoord = (Int, Int)
  type Bin3DCoord = (Int, Int, Int)

  def makeGaussianKernel(radius: Int, sigma: Double): Array[Array[Double]] = {
    val kernelDimension = calcKernelDimension(radius)
    val kernel = Array.ofDim[Double](kernelDimension, kernelDimension)
    var sum = 0.0

    for (x <- 0 until kernelDimension) {
      for (y <- 0 until kernelDimension) {
        val uc = x - (kernel.length - 1) / 2
        val vc = y - (kernel(0).length - 1) / 2
        // Calculate and save
        val g = Math.exp(-(uc * uc + vc * vc) / (2 * sigma * sigma))
        sum += g
        kernel(x)(y) = g
      }
    }

    // Normalize the kernel
    for (x <- 0 until kernel.length) {
      for (y <- 0 until kernel(0).length) {
        kernel(x)(y) /= sum
      }
    }

    kernel
  }

  def calcKernelDimension(radius: Int) = 2 * radius + 1
}
