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

import org.apache.spark.sql.DataFrame
import software.uncharted.xdata.spark.SparkFunSpec


// scalastyle:off multiple.string.literals magic.number
case class TopicTestData(lon: Double, lat: Double, time: Long, text: List[String])

class GeoTopicOpTest extends SparkFunSpec {

  private val lonCol = "lon"
  private val latCol = "lat"
  private val timeCol = "time"
  private val textCol = "text"

  private def createTestString(aCount: Int, bCount: Int) = List.fill(aCount)("a") ++ List.fill(bCount)("b")

  def genData: DataFrame = {
    val testData =
    // 1st time bucket
      List(TopicTestData(-91.0, -67.0, 101L, createTestString(1, 11)),
        TopicTestData(-89.0, -65.0, 101L, createTestString(2, 22)),
        TopicTestData(91.0, -65.0, 101L, createTestString(3, 33)),
        TopicTestData(89.0, -67.0, 101L, createTestString(4, 44)),
        TopicTestData(89.0, 65.0, 101L, createTestString(5, 55)),
        TopicTestData(91.0, 67.0, 101L, createTestString(6, 66)),
        TopicTestData(-91.0, 65.0, 101L, createTestString(7, 77)),
        TopicTestData(-89.0, 67.0, 101L, createTestString(8, 88)),
        // 2nd time bucket
        TopicTestData(-91.0, -67.0, 201L, createTestString(9, 99)),
        TopicTestData(-89.0, -65.0, 201L, createTestString(10, 1010)),
        TopicTestData(91.0, -65.0, 201L, createTestString(11, 1111)),
        TopicTestData(89.0, -67.0, 201L, createTestString(12, 1212)),
        TopicTestData(89.0, 65.0, 201L, createTestString(13, 1313)),
        TopicTestData(91.0, 67.0, 201L, createTestString(14, 1414)),
        TopicTestData(-91.0, 65.0, 201L, createTestString(15, 1515)),
        TopicTestData(-89.0, 67.0, 201L, createTestString(16, 1616)),
        // 3rd time bucket
        TopicTestData(-179, 84.0, 301L, createTestString(17, 1717)),
        TopicTestData(-179, 84.0, 301L, createTestString(18, 1818)))

    val tsqlc = sqlc
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("GeoTopicOpTest") {
    it("\"should create a quadtree of tiles where empty tiles are skipped") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, textCol, RangeDescription.fromCount(0, 800, 10), 3, 3, None)
      val result = GeoTopicOp(conf)(genData).collect().map(_.coords).toSet
      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3), (2, 0, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should create time bins from a range and bucket count") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, textCol, RangeDescription.fromCount(0, 800, 10), 3, 3, None)
      val result = GeoTopicOp(conf)(genData).collect()
      assertResult(10)(result(0).bins.length)
    }

    it("should sum values that are in the same same tile") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, textCol, RangeDescription.fromCount(0, 800, 10), 3, 1, None)
      val result = GeoTopicOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 1))
      assertResult(List("b" -> 3535, "a" -> 35))(result(0).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }

    it("should not aggregate across time buckets") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, textCol, RangeDescription.fromCount(0, 800, 10), 3, 3, None)
      val result = GeoTopicOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(List("b" -> 3535, "a" -> 35))(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }
  }
}
