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

/**
  * An abstract implementation of the Gaussian Blur. Applies a gaussian blur over a set of coordinates given the value at the coordinate.
  * Values for coordinates not passed in, are assumed to be 0
  *
  * @param radius The radius of the kernel to applied
  * @param sigma  The sigma value for the gaussian distribution
  * @param tms    if true, the Y axis for tile coordinates only is flipped
  */
abstract class GaussianBlurSpreadingFunction[BC](radius: Int, sigma: Double, tms: Boolean = true)
  extends SpreadingFunction[TileCoord, BC, Double] {

  protected val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
  protected val kernelDimension = GaussianBlurSpreadingFunction.calcKernelDimension(radius)

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

    coordsValueMap.map(applyKernel(coordsValueMap))
  }

  private def addNeighbouringBins(value: Double)
                                 (coords: (TileCoord, BC)): Map[(TileCoord, BC), Double] = {
    val tileCoord = coords._1
    val binCoord = coords._2
    var result = Map(((tileCoord, binCoord) -> value))

    // Translate kernel coordinates into tile and bin coordinates and add them to result
    for (kernelY <- 0 to kernelDimension - 1) {
      for (kernelX <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoord, binCoord, (kernelX, kernelY))
        if (!result.contains(kernelTileCoord, kernelBinCoord)) {
          result = result + ((kernelTileCoord, kernelBinCoord) -> 0.0) // Default value for coordinates not part of the original coordsTraversable is 0
        }
      }
    }

    result
  }

  private def applyKernel(coordsValueMapTraversable: Map[(TileCoord, BC), Double])
                         (coordsValueMap: ((TileCoord, BC), Double)): (TileCoord, BC, Option[Double]) = {
    val coords = coordsValueMap._1
    val data = coordsValueMap._2
    val tileCoord = coords._1
    val binCoord = coords._2
    var result = List[Double]()

    for (kernelY <- 0 to kernelDimension - 1) {
      for (kernelX <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoord, binCoord, (kernelX, kernelY))
        val coordValue = coordsValueMapTraversable.get((kernelTileCoord, kernelBinCoord))
        result = result :+ kernel(kernelY)(kernelX) * coordValue.getOrElse(0.0)
      }
    }

    (tileCoord, binCoord, Some(result.sum))
  }

  def calcKernelCoord(tileCoord: TileCoord, binCoord: BC, kernelIndex: (Int, Int)): (TileCoord, BC)

  protected def translateLeft(tileCoord: TileCoord) = TileCoord(tileCoord.z, tileCoord.x - 1, tileCoord.y)

  protected def translateRight(tileCoord: TileCoord) = TileCoord(tileCoord.z, tileCoord.x + 1, tileCoord.y)

  protected def translateUp(tileCoord: TileCoord) = if (tms) TileCoord(tileCoord.z, tileCoord.x, tileCoord.y + 1) else TileCoord(tileCoord.z, tileCoord.x, tileCoord.y - 1)

  protected def translateDown(tileCoord: TileCoord) = if (tms) TileCoord(tileCoord.z, tileCoord.x, tileCoord.y - 1) else TileCoord(tileCoord.z, tileCoord.x, tileCoord.y + 1)

  protected def isTileCoordValid(tileCoord: TileCoord) = {
    val maxTiles = (1 << tileCoord.z) - 1
    (tileCoord.x >= 0) && (tileCoord.y >= 0) && (tileCoord.x <= maxTiles) && (tileCoord.y <= maxTiles)
  }

  protected def isBinCoordValid(binCoord: BC): Boolean
}

/**
  * An implementation of the Gaussian Blur for bin coordinates that are 2 dimensional (x and y).
  *
  * @param radius  The radius of the kernel to applied
  * @param sigma   The sigma value for the gaussian distribution
  * @param maxBins The maximum number of bins in the x and y direction
  * @param tms     if true, the Y axis for tile coordinates only is flipped
  */
class GaussianBlurSpreadingFunction2D(radius: Int, sigma: Double, maxBins: Bin2DCoord, tms: Boolean = true)
  extends GaussianBlurSpreadingFunction[Bin2DCoord](radius: Int, sigma: Double, tms: Boolean) {

  def spread(coordsTraversable: Traversable[((Int, Int, Int), (Int, Int))], value: Option[Double]): Traversable[((Int, Int, Int), (Int, Int), Option[Double])] = {
    val typedCoordsTraversable = coordsTraversable.map(coord => (TileCoord.tupled(coord._1), Bin2DCoord.tupled(coord._2)))
    val spreadValues = super.spread(typedCoordsTraversable, value)
    spreadValues.map(value => (TileCoord.unapply(value._1).get, Bin2DCoord.unapply(value._2).get, value._3))
  }

  protected def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin2DCoord, kernelIndex: (Int, Int)): (TileCoord, Bin2DCoord) = {
    var kernelBinCoordX = binCoord.x + kernelIndex._1 - Math.floor(kernelDimension / 2).toInt
    var kernelBinCoordY = binCoord.y + kernelIndex._2 - Math.floor(kernelDimension / 2).toInt
    var kernelBinCoord = Bin2DCoord(kernelBinCoordX, kernelBinCoordY)
    var kernelTileCoord = tileCoord

    // If kernel bin coordinate lies outside of the tile, calculate new coordinates for tile and bin
    if (kernelBinCoordX < 0) {
      kernelTileCoord = translateLeft(kernelTileCoord)
      kernelBinCoord = calcBinCoordInLeftTile(kernelBinCoord)
    } else if (kernelBinCoordX > maxBins.x) {
      kernelTileCoord = translateRight(kernelTileCoord)
      kernelBinCoord = calcBinCoordInRightTile(kernelBinCoord)
    }

    if (kernelBinCoordY < 0) {
      kernelTileCoord = translateUp(kernelTileCoord)
      kernelBinCoord = calcBinCoordInTopTile(kernelBinCoord)
    } else if (kernelBinCoordY > maxBins.y) {
      kernelTileCoord = translateDown(kernelTileCoord)
      kernelBinCoord = calcBinCoordInBottomTile(kernelBinCoord)
    }

    (kernelTileCoord, kernelBinCoord)
  }

  private def calcBinCoordInLeftTile(kernelBinCoord: Bin2DCoord) = Bin2DCoord(maxBins.x + kernelBinCoord.x + 1, kernelBinCoord.y)

  private def calcBinCoordInRightTile(kernelBinCoord: Bin2DCoord) = Bin2DCoord(kernelBinCoord.x - maxBins.x - 1, kernelBinCoord.y)

  private def calcBinCoordInTopTile(kernelBinCoord: Bin2DCoord) = Bin2DCoord(kernelBinCoord.x, maxBins.y + kernelBinCoord.y + 1)

  private def calcBinCoordInBottomTile(kernelBinCoord: Bin2DCoord) = Bin2DCoord(kernelBinCoord.x, kernelBinCoord.y - maxBins.y - 1)

  protected def isBinCoordValid(binCoord: Bin2DCoord) = (binCoord.x >= 0) && (binCoord.y >= 0) && (binCoord.x <= maxBins.x) && (binCoord.y <= maxBins.y)
}

/**
  * An implementation of the Gaussian Blur for bin coordinates that are 3 dimensional (x, y, and z).
  * Useful for applying gaussian blur to XYTimeOps, where the z dimension represents time.
  * However, the third coordinate's value does not matter. Manipulations are done only on the x and y coordinates
  *
  * @param radius  The radius of the kernel to applied
  * @param sigma   The sigma value for the gaussian distribution
  * @param maxBins The maximum number of bins in the x and y direction
  * @param tms     if true, the Y axis for tile coordinates only is flipped
  */
class GaussianBlurSpreadingFunction3D(radius: Int, sigma: Double, maxBins: Bin2DCoord, tms: Boolean = true)
  extends GaussianBlurSpreadingFunction[Bin3DCoord](radius: Int, sigma: Double, tms: Boolean) {

  def spread(coordsTraversable: Traversable[((Int, Int, Int), (Int, Int, Int))], value: Option[Double]): Traversable[((Int, Int, Int), (Int, Int, Int), Option[Double])] = {
    val typedCoordsTraversable = coordsTraversable.map(coord => (TileCoord.tupled(coord._1), Bin3DCoord.tupled(coord._2)))
    val spreadValues = super.spread(typedCoordsTraversable, value)
    spreadValues.map(value => (TileCoord.unapply(value._1).get, Bin3DCoord.unapply(value._2).get, value._3))
  }

  protected def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin3DCoord, kernelIndex: (Int, Int)): (TileCoord, Bin3DCoord) = {
    var kernelBinCoordX = binCoord.x + kernelIndex._1 - Math.floor(kernelDimension / 2).toInt
    var kernelBinCoordY = binCoord.y + kernelIndex._2 - Math.floor(kernelDimension / 2).toInt
    var kernelBinCoord = Bin3DCoord(kernelBinCoordX, kernelBinCoordY, binCoord.z)
    var kernelTileCoord = tileCoord

    // If kernel bin coordinate lies outside of the tile, calculate new coordinates for tile and bin
    if (kernelBinCoordX < 0) {
      kernelTileCoord = translateLeft(kernelTileCoord)
      kernelBinCoord = calcBinCoordInLeftTile(kernelBinCoord)
    } else if (kernelBinCoordX > maxBins.x) {
      kernelTileCoord = translateRight(kernelTileCoord)
      kernelBinCoord = calcBinCoordInRightTile(kernelBinCoord)
    }

    if (kernelBinCoordY < 0) {
      kernelTileCoord = translateUp(kernelTileCoord)
      kernelBinCoord = calcBinCoordInTopTile(kernelBinCoord)
    } else if (kernelBinCoordY > maxBins.y) {
      kernelTileCoord = translateDown(kernelTileCoord)
      kernelBinCoord = calcBinCoordInBottomTile(kernelBinCoord)
    }

    (kernelTileCoord, kernelBinCoord)
  }

  private def calcBinCoordInLeftTile(kernelBinCoord: Bin3DCoord) = Bin3DCoord(maxBins.x + kernelBinCoord.x + 1, kernelBinCoord.y, kernelBinCoord.z)

  private def calcBinCoordInRightTile(kernelBinCoord: Bin3DCoord) = Bin3DCoord(kernelBinCoord.x - maxBins.x - 1, kernelBinCoord.y, kernelBinCoord.z)

  private def calcBinCoordInTopTile(kernelBinCoord: Bin3DCoord) = Bin3DCoord(kernelBinCoord.x, maxBins.y + kernelBinCoord.y + 1, kernelBinCoord.z)

  private def calcBinCoordInBottomTile(kernelBinCoord: Bin3DCoord) = Bin3DCoord(kernelBinCoord.x, kernelBinCoord.y - maxBins.y - 1, kernelBinCoord.z)

  protected def isBinCoordValid(binCoord: Bin3DCoord) = binCoord.x >= 0 && binCoord.y >= 0 && binCoord.x <= maxBins.x && binCoord.y <= maxBins.y
}

object GaussianBlurSpreadingFunction {
  protected def makeGaussianKernel(radius: Int, sigma: Double): Array[Array[Double]] = {
    val kernelDimension = calcKernelDimension(radius)
    val kernel = Array.ofDim[Double](kernelDimension, kernelDimension)
    var sum = 0.0

    for (y <- 0 until kernelDimension; x <- 0 until kernelDimension) {
      val uc = y - (kernel.length - 1) / 2
      val vc = x - (kernel(0).length - 1) / 2
      // Calculate and save
      val g = Math.exp(-(uc * uc + vc * vc) / (2 * sigma * sigma))
      sum += g
      kernel(y)(x) = g
    }

    // Normalize the kernel
    for (y <- 0 until kernelDimension; x <- 0 until kernelDimension) {
      kernel(y)(x) /= sum
    }

    kernel
  }

  protected def calcKernelDimension(radius: Int) = 2 * radius + 1
}

case class TileCoord(z: Int, x: Int, y: Int)

case class Bin2DCoord(x: Int, y: Int)

case class Bin3DCoord(x: Int, y: Int, z: Int)
