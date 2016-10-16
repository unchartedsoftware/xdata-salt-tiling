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

import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite

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
}
