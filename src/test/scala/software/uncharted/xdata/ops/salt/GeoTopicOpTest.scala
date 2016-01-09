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
case class TopicTestData(lon: Double, lat: Double, time: Long, counts: Map[String, Int])

class GeoTopicOpTest extends SparkFunSpec {

  private val lonCol = "lon"
  private val latCol = "lat"
  private val timeCol = "time"
  private val countsCol = "counts"

  def genData: DataFrame = {
    val testData =
    // 1st time bucket
      List(TopicTestData(-91.0, -67.0, 101L, Map("a" -> 1, "b" -> 11)),
        TopicTestData(-89.0, -65.0, 101L, Map("a" -> 2, "b" -> 22)),
        TopicTestData(91.0, -65.0, 101L, Map("a" -> 3, "b" -> 33)),
        TopicTestData(89.0, -67.0, 101L, Map("a" -> 4, "b" -> 44)),
        TopicTestData(89.0, 65.0, 101L, Map("a" -> 5, "b" -> 55)),
        TopicTestData(91.0, 67.0, 101L, Map("a" -> 6, "b" -> 66)),
        TopicTestData(-91.0, 65.0, 101L, Map("a" -> 7, "b" -> 77)),
        TopicTestData(-89.0, 67.0, 101L, Map("a" -> 8, "b" -> 88)),
        // 2nd time bucket
        TopicTestData(-91.0, -67.0, 201L, Map("a" -> 9, "b" -> 99)),
        TopicTestData(-89.0, -65.0, 201L, Map("a" -> 10, "b" -> 1010)),
        TopicTestData(91.0, -65.0, 201L, Map("a" -> 11, "b" -> 1111)),
        TopicTestData(89.0, -67.0, 201L, Map("a" -> 12, "b" -> 1212)),
        TopicTestData(89.0, 65.0, 201L, Map("a" -> 13, "b" -> 1313)),
        TopicTestData(91.0, 67.0, 201L, Map("a" -> 14, "b" -> 1414)),
        TopicTestData(-91.0, 65.0, 201L, Map("a" -> 15, "b" -> 1515)),
        TopicTestData(-89.0, 67.0, 201L, Map("a" -> 16, "b" -> 1616)),
        // 3rd time bucket
        TopicTestData(-179, 84.0, 301L, Map("a" -> 17, "b" -> 1717)),
        TopicTestData(-179, 84.0, 301L, Map("a" -> 18, "b" -> 1818)))

    val tsqlc = sqlc
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("GeoTopicOpTest") {
    it("\"should create a quadtree of tiles where empty tiles are skipped") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, countsCol, None, RangeDescription.fromCount(0, 800, 10), 3, 3, 10)
      val result = GeoTopicOp(conf)(genData).collect().map(_.coords).toSet
      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3), (2, 0, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should create time bins from a range and bucket count") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, countsCol, None, RangeDescription.fromCount(0, 800, 10), 3, 3, 10)
      val result = GeoTopicOp(conf)(genData).collect()
      assertResult(10 * 10 * 10)(result(0).bins.length)
    }

    it("should sum values that are in the same bin ") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, countsCol, None, RangeDescription.fromCount(0, 800, 10), 3, 1, 10)
      val result = GeoTopicOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))
      assertResult(List("b" -> 3535, "a" -> 35))(result(0).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
    }

    it("should not aggregate across time buckets") {
      val conf = GeoTopicOpConf(lonCol, latCol, timeCol, countsCol, None, RangeDescription.fromCount(0, 800, 10), 3, 3, 10)
      val result = GeoTopicOp(conf)(genData).collect()
      val proj = new MercatorTimeProjection(RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(List("b" -> 3535, "a" -> 35))(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (9, 9, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 1, 3), (9, 9, 9))))
    }
  }
}
