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

package software.uncharted.salt.xdata.projection.geometry

import java.text.DecimalFormat

import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite
import software.uncharted.salt.xdata.projection.{IPProjection, IPSegmentProjection, SimpleLineProjection}

class IPProjectionTests extends FunSuite {
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1E-12)

  test("#isIPv4") {
    import IPProjection.isIPv4
    assert(isIPv4("172.168.0.1"))
    assert(!isIPv4("172.168.0.a"))
    assert(!isIPv4("172.168.0.1.0"))
    assert(!isIPv4("172.168.0"))
    assert(!isIPv4("bad man"))
  }

  test("#parseIPv4") {
    import IPProjection.parseIPv4
    assert(List(172, 168, 0, 1) === parseIPv4(Some("172.168.0.1")).get.toList)
    assert(parseIPv4(Some("172.168.0.a")).isEmpty)
    assert(parseIPv4(Some("172.168.0.1.0")).isEmpty)
    assert(parseIPv4(Some("172.168.0")).isEmpty)
    assert(parseIPv4(Some("bad man")).isEmpty)
  }

  test("#isIPv6") {
    import IPProjection.isIPv6
    assert(isIPv6("::"))
    assert(isIPv6("0::"))
    assert(isIPv6("a0f::"))
    assert(isIPv6("ffff:ffff:ffff::"))
    assert(isIPv6("abcd:::12:12aa:ffff"))
    assert(isIPv6("FFFF:ffff:FFFF:ffff:FFFF:ffff"))
    assert(!isIPv6("FFFF:ffff:FFFF:ffff:FFFF:ffff:ffff"))
    assert(!isIPv6("a0g::"))
    assert(!isIPv6("ffff:ffff"))
  }

  test("#parseIPv6") {
    import IPProjection.parseIPv6
    assert(List(0, 0, 0, 0, 0, 0) === parseIPv6(Some("::")).get.toList)
    assert(List(0, 0, 0, 0, 0, 0) === parseIPv6(Some("0::")).get.toList)
    assert(List(0xa0f, 0, 0, 0, 0, 0) === parseIPv6(Some("a0f::")).get.toList)
    assert(List(0xffff, 0xffff, 0xffff, 0, 0, 0) === parseIPv6(Some("ffff:ffff:ffff::")).get.toList)
    assert(List(0xabcd, 0, 0, 0x12, 0x12aa, 0xffff) === parseIPv6(Some("abcd:::12:12aa:ffff")).get.toList)
    assert(List(0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff) === parseIPv6(Some("FFFF:ffff:FFFF:ffff:FFFF:ffff")).get.toList)
    assert(parseIPv6(Some("FFFF:ffff:FFFF:ffff:FFFF:ffff:ffff")).isEmpty)
    assert(parseIPv6(Some("a0g::")).isEmpty)
    assert(parseIPv6(Some("ffff:ffff")).isEmpty)
  }

  test("#splitNum") {
    import IPProjection.splitNum
    assert((0, 0) === splitNum(8)(0))
    assert((7, 7) === splitNum(8)(63))
    assert((8, 0) === splitNum(8)(64))
    assert((15, 7) === splitNum(8)(127))
    assert((0, 8) === splitNum(8)(128))
    assert((7, 15) === splitNum(8)(191))
    assert((8, 8) === splitNum(8)(192))
    assert((15, 15) === splitNum(8)(255))
  }

  test("#arrayToSpaceFillingCurve with IPv4") {
    import IPProjection.arrayToSpaceFillingCurve
    assert((0.0, 0.0) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(0, 0, 0, 0)))
    assert((0.5, 0.0) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(64, 0, 0, 0)))
    assert((0.0, 0.5) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(128, 0, 0, 0)))
    assert((0.5, 0.5) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(192, 0, 0, 0)))

    assert((0.25000, 0.25000) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(48,   0,   0,   0)))
    assert((0.12500, 0.12500) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array(12,   0,   0,   0)))
    assert((0.06250, 0.06250) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array( 3,   0,   0,   0)))
    assert((0.03125, 0.03125) === arrayToSpaceFillingCurve(4, 8, (1.0, 1.0))(Array( 0, 192,   0,   0)))
  }

  test("#arrayToSpaceFillingCurve with IPv6") {
    import IPProjection.arrayToSpaceFillingCurve
    assert((0.0, 0.0) === arrayToSpaceFillingCurve(6, 16, (1.0, 1.0))(Array(0x0000, 0, 0, 0, 0, 0)))
    assert((0.5, 0.0) === arrayToSpaceFillingCurve(6, 16, (1.0, 1.0))(Array(0x4000, 0, 0, 0, 0, 0)))
    assert((0.0, 0.5) === arrayToSpaceFillingCurve(6, 16, (1.0, 1.0))(Array(0x8000, 0, 0, 0, 0, 0)))
    assert((0.5, 0.5) === arrayToSpaceFillingCurve(6, 16, (1.0, 1.0))(Array(0xc000, 0, 0, 0, 0, 0)))
  }

  test("point projection from IP addresses") {
    val projection = new IPProjection(Seq(4))

    // 172.168.0.1 is:
    // ac.a8.0.1 is:
    // 10101100.10101000.00000000.00000001
    // which splits to
    //  0 0 1 0. 0 0 0 0. 0 0 0 0. 0 0 0 1 and
    // 1 1 1 0 .1 1 1 0 .0 0 0 0 .0 0 0 0
    // or (0x2001, 0xee00) = (8193, 60928) = (0.1250152587890625, 0.9296875)
    // level 4 has 16 x 16 tiles, each with 4 x 4 bins, so this falls in bin (8, 59), or tile (4, 2, 14), in bin (0, 3)
    // However, y axis is reversed for bins, so this becomes ((4, 2, 14)(0, 0))
    assert(((4, 2, 14), (0, 0)) === projection.project(Some("172.168.0.1"), (3, 3)).get.toList.apply(0))
  }

  test("segment projection from IP addresses") {
    val projection = new IPSegmentProjection(Seq(4), new SimpleLineProjection(Seq(4), (0.0, 0.0), (1.0, 1.0), tms=true))
    // we want a segment from (4, 4, 2)(0, 2) to (4, 4, 2)(3, 3)
    // reverse bin Y's to get (4, 4, 2)(0, 1) to (4, 4, 2)(3, 0)
    // becomes universal bins (16, 9) to (19, 8)
    // put in middle of bins: (16.5, 9.5) to (19.5, 8.5)
    // scale to [0, 1): (0.2578125 0.1484375) to (0.3046875 0.1328125)
    // in split IP coords, this is (16896 9728) to (19968 8704) or (0x4200, 0x2600) to (0x4e00, 0x2200)
    // interleaved, this becomes:
    // 0x4200 =  0 1 0 0. 0 0 1 0. 0 0 0 0. 0 0 0 0
    // 0x2600 = 0 0 1 0 .0 1 1 0 .0 0 0 0 .0 0 0 0
    //       == 00011000.00101100.00000000.00000000
    //        = 0x18.0x2a.0.0
    //        = 24.42.0.0
    // 0x4e00 =  0 1 0 0. 1 1 1 0. 0 0 0 0. 0 0 0 0
    // 0x2200 = 0 0 1 0 .0 0 1 0 .0 0 0 0 .0 0 0 0
    //       == 00011000.01011100.00000000.00000000
    //        = 0x18.0x5c.0.0
    //        = 24.90.0.0

    val result = projection.project(Some(("24.42.0.0", "24.90.0.0")), (3, 3)).get.toList
    assert(4 === result.length)
    assert(((4, 4, 2), (0, 2)) === result(0))
    assert(((4, 4, 2), (1, 2)) === result(1))
    assert(((4, 4, 2), (2, 3)) === result(2))
    assert(((4, 4, 2), (3, 3)) === result(3))
  }

  test("IPv6 addresses to Cartesian coordinates") {
    val maxBin = (3, 3)

    val emptyIPv6 = "::"
    val centreCoords = IPProjection.ipToCartesian(Some(emptyIPv6))
    assertResult(Some((0.0,0.0)))(centreCoords)

    val nonEmptyIPv6 = "FFFF:ffff:FFFF:ffff:FFFF:ffff"
    val resultCoords = IPProjection.ipToCartesian(Some(nonEmptyIPv6)).get
    val df = new DecimalFormat("#.#")
    assert(df.format(resultCoords._1) == "1")
    assert(df.format(resultCoords._2) == "1")
  }
}
