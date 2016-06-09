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


case class CartesianTopicTestData(x: Double, y: Double, time: Long, text: List[String])

class CartesianTimeTopicTest extends SparkFunSpec {
  private val xCol = "x"
  private val yCol = "y"
  private val timeCol = "time"
  private val textCol = "text"

  // scalastyle:off multiple.string.literals magic.number
  private def createTestString(aCount: Int, bCount: Int) = List.fill(aCount)("a") ++ List.fill(bCount)("b")

  def genData: DataFrame = {
    val testData =
    // 1st time bucket
    List(
        CartesianTopicTestData(0.24, 0.24, 101L, createTestString(1, 11)),
        CartesianTopicTestData(0.6, 0.24, 101L, createTestString(2, 22)),
        CartesianTopicTestData(0.26, 0.26, 101L, createTestString(3, 33)),
        CartesianTopicTestData(0.76, 0.26, 101L, createTestString(4, 44)),
        CartesianTopicTestData(0.24, 0.6, 101L, createTestString(5, 55)),
        CartesianTopicTestData(0.6, 0.6, 101L, createTestString(6, 66)),
        CartesianTopicTestData(0.26, 0.76, 101L, createTestString(7, 77)),
        CartesianTopicTestData(0.76, 0.76, 101L, createTestString(8, 88)),
        // 2nd time bucket
        CartesianTopicTestData(0.24, 0.24, 201L, createTestString(9, 9)),
        CartesianTopicTestData(0.6, 0.24, 201L, createTestString(10, 1010)),
        CartesianTopicTestData(0.26, 0.26, 201L, createTestString(11, 1111)),
        CartesianTopicTestData(0.76, 0.26, 201L, createTestString(12, 1212)),
        CartesianTopicTestData(0.24, 0.6, 201L, createTestString(13, 1313)),
        CartesianTopicTestData(0.6, 0.6, 201L, createTestString(14, 1414)),
        CartesianTopicTestData(0.26, 0.76, 201L, createTestString(15, 1515)),
        CartesianTopicTestData(0.76, 0.76, 201L, createTestString(16, 1616)),
        // 3rd time bucket
        CartesianTopicTestData(0.01, 0.99, 301L, createTestString(17, 1717)),
        CartesianTopicTestData(0.01, 0.99, 301L, createTestString(18, 1818)))

    val tsqlc = sqlc
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("CartesianTimeTopics") {
    it("\"should create a quadtree of tiles where empty tiles are skipped") {
      val result = CartesianTimeTopics(xCol, yCol, timeCol, textCol, Some((0.0, 0.0, 1.0, 1.0)), RangeDescription.fromCount(0, 800, 10), 3, 0 until 3)(genData)
        .collect()
        .map(_.coords)
        .toSet
      val expectedSet = Set(
        (0,0,0), // l0
        (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
        (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3), (2, 0, 3)) // l2
      assertResult((Set(), Set()))((expectedSet diff result, result diff expectedSet))
    }

    it("should create time bins from a range and bucket count") {
      val result = CartesianTimeTopics(xCol, yCol, timeCol, textCol, Some((0.0, 0.0, 1.0, 1.0)), RangeDescription.fromCount(0, 800, 10), 3, 0 until 3)(genData).collect()
      assertResult(10)(result(0).bins.length())
    }

    it("should sum values that are in the same same tile") {
      val result = CartesianTimeTopics(xCol, yCol, timeCol, textCol, Some((0.0, 0.0, 1.0, 1.0)), RangeDescription.fromCount(0, 800, 10), 3, Seq(0))(genData).collect()
      val proj = new CartesianTimeProjection(Seq(0), (0.0, 0.0), (1.0, 1.0), RangeDescription.fromCount(0L, 800L, 10))
      assertResult(List("b" -> 3535, "a" -> 35))(result(0).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }

    it("should not aggregate across time buckets") {
      val result = CartesianTimeTopics(xCol, yCol, timeCol, textCol, Some((0.0, 0.0, 1.0, 1.0)), RangeDescription.fromCount(0, 800, 10), 3, 0 until 3)(genData).collect()
      val proj = new CartesianTimeProjection(Seq(0), (0.0, 0.0), (1.0, 1.0), RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(List("b" -> 3535, "a" -> 35))(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }
  }
}
