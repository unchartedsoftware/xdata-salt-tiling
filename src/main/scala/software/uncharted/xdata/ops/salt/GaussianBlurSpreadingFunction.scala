package software.uncharted.xdata.ops.salt

import software.uncharted.salt.core.spreading.SpreadingFunction
import software.uncharted.xdata.geometry.CartesianBinning
import software.uncharted.xdata.ops.salt.GaussianBlurSpreadingFunction.{Bin2DCoord, Bin3DCoord, TileCoord}

// TODO: What was the purpose of extending CartesianBinning?
class GaussianBlurSpreadingFunction(radius: Int, sigma: Double, maxBins: Bin2DCoord)
  extends SpreadingFunction[TileCoord, Bin2DCoord, Double] {

  private val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
  private val kernelDimension = GaussianBlurSpreadingFunction.calcKernelDimension(radius)

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value             the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  // TODO: Are there types for tile coordinate, bin coordinate?
  // TODO: Should those types take into acct 3 dimensional bin coordinates which are present in
  // TODO: Confirm that the default value is 1.0
  override def spread(coordsTraversable: Traversable[(TileCoord, Bin2DCoord)], value: Option[Double]): Traversable[(TileCoord, Bin2DCoord, Option[Double])] = {
    val coordsValueMap = coordsTraversable
      .flatMap(addNeighbouringBins(value.getOrElse(1.0))) // Add bins in neighborhood, affected by gaussian blur
      .groupBy(coordsValueMap => coordsValueMap._1) // Group by key. Key: (tileCoordinate, BinCoordinate2D)
      .map({ case (group, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }) // Reduces by key, adding the values

    // TODO: Can I think of a better way to write this
    coordsValueMap.map(applyKernel(coordsValueMap))
  }

  def addNeighbouringBins(value: Double)
                         (coords: (TileCoord, Bin2DCoord)): Map[(TileCoord, Bin2DCoord), Double] = {
    val tileCoordinate = coords._1
    val BinCoordinate2D = coords._2
    var result = Map(((tileCoordinate, BinCoordinate2D) -> value))

    // Translate kernel coordinates into tile and bin coordinates
    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, BinCoordinate2D, (kernelX, kernelY))
        if (!result.contains(kernelTileCoord, kernelBinCoord)) {
          result = result + ((kernelTileCoord, kernelBinCoord) -> 0.0)
        }
      }
    }

    result
  }

  // TODO: Can I think of a better name for coordsValueMap (the iterable) and coordsValue (the tuple?)
  def applyKernel(coordsValueMap: Map[(TileCoord, Bin2DCoord), Double])
                 (coordsValue: ((TileCoord, Bin2DCoord), Double)): (TileCoord, Bin2DCoord, Option[Double]) = {
    val coords = coordsValue._1
    val value = coordsValue._2
    val tileCoordinate = coords._1
    val binCoordinate = coords._2
    var result = List[Double]()

    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, binCoordinate, (kernelX, kernelY))
        val valueAtCoord = coordsValueMap.get((kernelTileCoord, kernelBinCoord))
        result = result :+ kernel(kernelX)(kernelY) * valueAtCoord.getOrElse(0.0)
      }
    }

    (tileCoordinate, binCoordinate, Some(result.sum))
  }

  def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin2DCoord, kernelIndex: Bin2DCoord): (TileCoord, Bin2DCoord) = {
    var kernelBinCoord = (binCoord._1 + kernelIndex._1 - Math.floorDiv(kernelDimension, 2), binCoord._2 + kernelIndex._2 - Math.floorDiv(kernelDimension, 2))
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

  // TODO: Consider, will tms need to be taken into acct here?
  def translateLeft(tileCoordinate: TileCoord) = (tileCoordinate._1 - 1, tileCoordinate._2, tileCoordinate._3)

  def translateRight(tileCoordinate: TileCoord) = (tileCoordinate._1 + 1, tileCoordinate._2, tileCoordinate._3)

  def translateUp(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 + 1, tileCoordinate._3)

  def translateDown(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 - 1, tileCoordinate._3)

  def calcBinCoordInLeftTile(kernelBinCoord: Bin2DCoord) = (maxBins._1 + kernelBinCoord._1 + 1, kernelBinCoord._2)

  def calcBinCoordInRightTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1 - maxBins._1 - 1, kernelBinCoord._2)

  def calcBinCoordInTopTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1, maxBins._1 + kernelBinCoord._2 + 1)

  def calcBinCoordInBottomTile(kernelBinCoord: Bin2DCoord) = (kernelBinCoord._1, kernelBinCoord._2 - maxBins._1 - 1)
}

class GaussianBlurSpreadingFunction3D(radius: Int, sigma: Double, maxBins: Bin3DCoord)
  extends SpreadingFunction[TileCoord, Bin3DCoord, Double] {

  private val kernel = GaussianBlurSpreadingFunction.makeGaussianKernel(radius, sigma)
  private val kernelDimension = GaussianBlurSpreadingFunction.calcKernelDimension(radius)

  /**
    * Spread a single value over multiple visualization-space coordinates
    *
    * @param coordsTraversable the visualization-space coordinates
    * @param value             the value to spread
    * @return Seq[(TC, BC, Option[T])] A sequence of tile coordinates, with the spread values
    */
  // TODO: Are there types for tile coordinate, bin coordinate?
  // TODO: Should those types take into acct 3 dimensional bin coordinates which are present in
  // TODO: Confirm that the default value is 1.0
  override def spread(coordsTraversable: Traversable[(TileCoord, Bin3DCoord)], value: Option[Double]): Traversable[(TileCoord, Bin3DCoord, Option[Double])] = {
    val coordsValueMap = coordsTraversable
      .flatMap(addNeighbouringBins(value.getOrElse(1.0))) // Add bins in neighborhood, affected by gaussian blur
      .groupBy(coordsValueMap => coordsValueMap._1) // Group by key. Key: (tileCoordinate, BinCoordinate2D)
      .map({ case (group, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }) // Reduces by key, adding the values

    // TODO: Can I think of a better way to write this
    coordsValueMap.map(applyKernel(coordsValueMap))
  }

  def addNeighbouringBins(value: Double)
                         (coords: (TileCoord, Bin3DCoord)): Map[(TileCoord, Bin3DCoord), Double] = {
    val tileCoordinate = coords._1
    val binCoordinate = coords._2
    var result = Map(((tileCoordinate, binCoordinate) -> value))

    // Translate kernel coordinates into tile and bin coordinates
    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, binCoordinate, (kernelX, kernelY, binCoordinate._3))
        if (!result.contains(kernelTileCoord, kernelBinCoord)) {
          result = result + ((kernelTileCoord, kernelBinCoord) -> 0.0)
        }
      }
    }

    result
  }

  // TODO: Can I think of a better name for coordsValueMap (the iterable) and coordsValue (the tuple?)
  def applyKernel(coordsValueMap: Map[(TileCoord, Bin3DCoord), Double])
                 (coordsValue: ((TileCoord, Bin3DCoord), Double)): (TileCoord, Bin3DCoord, Option[Double]) = {
    val coords = coordsValue._1
    val value = coordsValue._2
    val tileCoordinate = coords._1
    val binCoordinate = coords._2
    var result = List[Double]()

    for (kernelX <- 0 to kernelDimension - 1) {
      for (kernelY <- 0 to kernelDimension - 1) {
        val (kernelTileCoord, kernelBinCoord) = calcKernelCoord(tileCoordinate, binCoordinate, (kernelX, kernelY, binCoordinate._3))
        val valueAtCoord = coordsValueMap.get((kernelTileCoord, kernelBinCoord))
        result = result :+ kernel(kernelX)(kernelY) * valueAtCoord.getOrElse(0.0)
      }
    }

    (tileCoordinate, binCoordinate, Some(result.sum))
  }

  def calcKernelCoord(tileCoord: TileCoord, binCoord: Bin3DCoord, kernelIndex: Bin3DCoord): (TileCoord, Bin3DCoord) = {
    var kernelBinCoord = (binCoord._1 + kernelIndex._1 - Math.floorDiv(kernelDimension, 2), binCoord._2 + kernelIndex._2 - Math.floorDiv(kernelDimension, 2), binCoord._3)
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

  // TODO: Consider, will tms need to be taken into acct here?
  def translateLeft(tileCoordinate: TileCoord) = (tileCoordinate._1 - 1, tileCoordinate._2, tileCoordinate._3)

  def translateRight(tileCoordinate: TileCoord) = (tileCoordinate._1 + 1, tileCoordinate._2, tileCoordinate._3)

  def translateUp(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 + 1, tileCoordinate._3)

  def translateDown(tileCoordinate: TileCoord) = (tileCoordinate._1, tileCoordinate._2 - 1, tileCoordinate._3)

  def calcBinCoordInLeftTile(kernelBinCoord: Bin3DCoord) = (maxBins._1 + kernelBinCoord._1 + 1, kernelBinCoord._2, kernelBinCoord._3)

  def calcBinCoordInRightTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1 - maxBins._1 - 1, kernelBinCoord._2, kernelBinCoord._3)

  def calcBinCoordInTopTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1, maxBins._1 + kernelBinCoord._2 + 1, kernelBinCoord._3)

  def calcBinCoordInBottomTile(kernelBinCoord: Bin3DCoord) = (kernelBinCoord._1, kernelBinCoord._2 - maxBins._1 - 1, kernelBinCoord._3)
}

object GaussianBlurSpreadingFunction {
  type TileCoord = (Int, Int, Int)
  type Bin2DCoord = (Int, Int)
  type Bin3DCoord = (Int, Int, Int)

  def makeGaussianKernel(radius: Int, sigma: Double): Array[Array[Double]] = {
    val kernelDimension = calcKernelDimension(radius)
    val kernel = Array.ofDim[Double](kernelDimension, kernelDimension)
    var sum = 0.0

    for (u <- 0 until kernelDimension) {
      for (v <- 0 until kernelDimension) {
        val uc = u - (kernel.length - 1) / 2
        val vc = v - (kernel(0).length - 1) / 2
        // Calculate and save
        val g = Math.exp(-(uc * uc + vc * vc) / (2 * sigma * sigma))
        sum += g
        kernel(u)(v) = g
      }
    }

    // Normalize the kernel
    for (u <- 0 until kernel.length) {
      for (v <- 0 until kernel(0).length) {
        kernel(u)(v) /= sum
      }
    }
    kernel
  }

  def calcKernelDimension(radius: Int) = 2 * radius + 1

  // TODO: Remove this at end
  def main(args: Array[String]): Unit = {
    val spreadingFunction = new GaussianBlurSpreadingFunction(4, 3.0, (255, 255))
    val kernel = makeGaussianKernel(4, 3.0)
    val coordMap = spreadingFunction.spread(List(
      ((1, 1, 1), (150, 151)),
      ((1, 1, 1), (150, 152)),
      ((1, 1, 1), (150, 153)),
      ((1, 1, 1), (150, 154)),
      ((1, 1, 1), (150, 155)),
      ((1, 1, 1), (150, 156)),
      ((1, 1, 1), (150, 157)),
      ((1, 1, 1), (150, 158)),
      ((1, 1, 1), (150, 159)),
      ((1, 1, 1), (150, 160)),
      ((1, 1, 1), (150, 161)),
      ((1, 1, 1), (150, 162)),
      ((1, 1, 1), (150, 163)),
      ((1, 1, 1), (150, 164))
    ), Some(1.0))

    coordMap.toList.sortBy((coord) => (coord._2._2, coord._2._1)).foreach(println)
  }
}
