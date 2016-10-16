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
package software.uncharted.xdata.geometry



import software.uncharted.salt.core.projection.Projection
import software.uncharted.salt.core.projection.numeric.{CartesianProjection, NumericProjection}

import scala.util.Try



/**
  * A projection from IP space to 2-d cartesian space
  */
class IPProjection (zoomLevels: Seq[Int]) extends Projection[String, (Int, Int, Int), (Int, Int)] {
  import IPProjection._
  type TileCoordinate = (Int, Int, Int)
  type BinCoordinate = (Int, Int)

  private val cartesian = new CartesianProjection(zoomLevels, MIN_DATA_COORDS, MAX_DATA_COORDS)


  override def project(rawCoordinate: Option[String], maxBin: BinCoordinate): Option[Traversable[(TileCoordinate, BinCoordinate)]] = {
    val coords = (
      parseIPv4(rawCoordinate).map { ipv4coords =>
        arrayToSpaceFillingCurve(4, 8, (0.0, 1.0))(ipv4coords)
      }
      ).map(Some(_)).getOrElse(
      parseIPv6(rawCoordinate).map { ipv6coords =>
        arrayToSpaceFillingCurve(6, 16, (0.0, 1.0))(ipv6coords)
      }
    )
    cartesian.project(coords, maxBin)
  }

  override def binTo1D(bin: BinCoordinate, maxBin: BinCoordinate): Int = {
    cartesian.binTo1D(bin, maxBin)
  }
}
object IPProjection {
  private val IPV4_PARTS = 4
  private val MIN_IPV4 = 0
  private val MAX_IPV4 = 0x100
  private val IPV6_PARTS = 6
  private val MIN_IPV6 = 0
  private val MAX_IPV6 = 0x10000
  private val MIN_DATA_COORDS = (0.0, 0.0)
  private val MAX_DATA_COORDS = (1.0, 1.0)

  private def inRange(lowInclusive: Int, highExclusive: Int)(value: Int): Boolean = {
    lowInclusive <= value && value < highExclusive
  }
  def isIPv4 (value: String): Boolean = {
    // Needs 4 parts, each an integer 0-255
    value.split("\\.").map(entry =>
      Try(entry.toInt).toOption.map(inRange(MIN_IPV4, MAX_IPV4)(_)).getOrElse(false)
    ).toList == List(true, true, true, true)
  }
  def parseIPv4 (valueOption: Option[String]): Option[Array[Int]] = {
    valueOption.flatMap{value =>
      val parts = value.split("\\.").map(entry => Try(entry.toInt).toOption)
      if (4 == parts.length && parts.map(_.map(inRange(MIN_IPV4, MAX_IPV4)(_)).getOrElse(false)).reduce(_ && _)) {
        Some(parts.map(_.get))
      } else {
        None
      }
    }
  }
  def isIPv6 (value: String): Boolean = {
    // Needs up to 6 parts, each blank or a hex int 0-ffff
    val entryValidity = value.split(":").map(entry =>
      "" == entry || Try(Integer.valueOf(entry, 16).intValue()).toOption.map(inRange(MIN_IPV6, MAX_IPV6)(_)).getOrElse(false)
    ).toList
    (entryValidity.length == 6 || value.endsWith("::")) &&
      entryValidity.length <= 6 && entryValidity.fold(true)(_ && _)
  }
  def parseIPv6 (valueOption: Option[String]): Option[Array[Int]] = {
    valueOption.flatMap{value =>
      val parts = value.split(":").map { entry =>
        if (entry.isEmpty) {
          Some(0)
        } else {
          Try(Integer.valueOf(entry, 16).intValue).toOption
        }
      }
      if (
        (parts.length == 6 || value.endsWith("::")) &&
          (parts.length <= 6 && parts.map(_.map(inRange(MIN_IPV6, MAX_IPV6)).getOrElse(false)).fold(true)(_ && _))
      ) {
        Some(parts.map(_.get) ++ Array.fill(IPV6_PARTS - parts.length)(0))
      } else {
        None
      }
    }
  }

  private[geometry] def bitsIn (n: Int): Int =
    if (n > 1) {
      1 + bitsIn(n >> 1)
    } else {
      0
    }

  private[geometry] def splitNum (numBits: Int)(value: Int): (Int, Int) = {
    val vCAR = value & 3
    val rCAR = ((vCAR & 1), (vCAR & 2) >> 1)
    if (numBits > 2) {
      val vCDR = value >> 2
      val rCDR = splitNum(numBits-2)(vCDR)
      (rCDR._1 * 2 + rCAR._1, rCDR._2 * 2 + rCAR._2)
    } else {
      rCAR
    }
  }

  /**
    * Split an array of values into x and y coordinates, using a space-filling z-curve
    *
    * @param arraySize The number of elements in the array
    * @param bitsPerEntry The number of bits per entry in the array
    * @param outputRange The total output range into which to scale the result
    * @param value The value to project
    * @return A projected cartesian value
    */
  private[geometry] def arrayToSpaceFillingCurve (arraySize: Int,
                                                  bitsPerEntry: Int,
                                                  outputRange: (Double, Double))
                                                 (value: Array[Int]): (Double, Double) = {
    val totalBound = (1L << (arraySize * bitsPerEntry / 2)).toDouble
    val entryBound = 1 << (bitsPerEntry / 2)
    val (xValues, yValues) = value.map(splitNum(bitsPerEntry)(_)).unzip

    val x = xValues.foldLeft(0L)(entryBound * _ + _)
    val y = yValues.foldLeft(0L)(entryBound * _ + _)

    (x * outputRange._1 / totalBound, y * outputRange._1 / totalBound)
  }
}
