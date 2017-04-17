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

package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.sql.DataFrame
import software.uncharted.salt.xdata.projection.MercatorTimeProjection
import software.uncharted.salt.xdata.util.RangeDescription
import software.uncharted.sparkpipe.ops.xdata.io.intScoreListToByteArray
import software.uncharted.xdata.spark.SparkFunSpec

case class MercatorTimeTopicTestData(lon: Double, lat: Double, time: Long, text: List[String])

class MercatorTimeTopicTest extends SparkFunSpec {

  private val lonCol = "lon"
  private val latCol = "lat"
  private val timeCol = "time"
  private val textCol = "text"

  // scalastyle:off multiple.string.literals magic.number
  private def createTestString(aCount: Int, bCount: Int) = List.fill(aCount)("a") ++ List.fill(bCount)("b")

  def genData: DataFrame = {
    val testData =
    // 1st time bucket
      List(MercatorTimeTopicTestData(-91.0, -67.0, 101L, createTestString(1, 11)),
        MercatorTimeTopicTestData(-89.0, -65.0, 101L, createTestString(2, 22)),
        MercatorTimeTopicTestData(91.0, -65.0, 101L, createTestString(3, 33)),
        MercatorTimeTopicTestData(89.0, -67.0, 101L, createTestString(4, 44)),
        MercatorTimeTopicTestData(89.0, 65.0, 101L, createTestString(5, 55)),
        MercatorTimeTopicTestData(91.0, 67.0, 101L, createTestString(6, 66)),
        MercatorTimeTopicTestData(-91.0, 65.0, 101L, createTestString(7, 77)),
        MercatorTimeTopicTestData(-89.0, 67.0, 101L, createTestString(8, 88)),
        // 2nd time bucket
        MercatorTimeTopicTestData(-91.0, -67.0, 201L, createTestString(9, 99)),
        MercatorTimeTopicTestData(-89.0, -65.0, 201L, createTestString(10, 1010)),
        MercatorTimeTopicTestData(91.0, -65.0, 201L, createTestString(11, 1111)),
        MercatorTimeTopicTestData(89.0, -67.0, 201L, createTestString(12, 1212)),
        MercatorTimeTopicTestData(89.0, 65.0, 201L, createTestString(13, 1313)),
        MercatorTimeTopicTestData(91.0, 67.0, 201L, createTestString(14, 1414)),
        MercatorTimeTopicTestData(-91.0, 65.0, 201L, createTestString(15, 1515)),
        MercatorTimeTopicTestData(-89.0, 67.0, 201L, createTestString(16, 1616)),
        // 3rd time bucket
        MercatorTimeTopicTestData(-179, 84.0, 301L, createTestString(17, 1717)),
        MercatorTimeTopicTestData(-179, 84.0, 301L, createTestString(18, 1818)))

    val tsqlc = sparkSession
    import tsqlc.implicits._ // scalastyle:ignore

    sc.parallelize(testData).toDF()
  }

  describe("MercatorTimeTopics") {
    it("should create a quadtree of tiles where empty tiles are skipped") {
      val result = MercatorTimeTopics(latCol, lonCol, timeCol, textCol, None, RangeDescription.fromCount(0, 800, 10), 3, 0 until 3, 1)(genData)
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
      val result = MercatorTimeTopics(latCol, lonCol, timeCol, textCol, None, RangeDescription.fromCount(0, 800, 10), 3, 0 until 3, 1)(genData).collect()
      assertResult(10)(result(0).bins.length())
    }

    it("should sum values that are in the same same tile") {
      val result = MercatorTimeTopics(latCol, lonCol, timeCol, textCol, None, RangeDescription.fromCount(0, 800, 10), 3, Seq(0), 1)(genData).collect()
      val proj = new MercatorTimeProjection(Seq(0), RangeDescription.fromCount(0L, 800L, 10))
      assertResult(List("b" -> 3535, "a" -> 35))(result(0).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }

    it("should not aggregate across time buckets") {
      val result = MercatorTimeTopics(latCol, lonCol, timeCol, textCol, None, RangeDescription.fromCount(0, 800, 10), 3, 0 until 3, 1)(genData).collect()
      val proj = new MercatorTimeProjection(Seq(0), RangeDescription.fromCount(0L, 800L, 10))

      val tile = (t: (Int, Int, Int)) => result.find(s => s.coords == t)

      assertResult(List("b" -> 3535, "a" -> 35))(tile((0, 0, 0)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((1, 0, 1)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
      assertResult(List("b" -> 3535, "a" -> 35))(tile((2, 0, 3)).getOrElse(fail()).bins(proj.binTo1D((0, 0, 3), (0, 0, 9))))
    }

    it ("should verify contents of tiles") {
      val session = sparkSession
      import session.implicits._ // scalastyle:ignore

      val testData =
        List(MercatorTimeTopicTestData(-99.0, 15.0, 101L, List("c", "c", "d", "d", "c", "c")),//bin 1D coord: 15
          MercatorTimeTopicTestData(-99.0, 40.0, 101L, List("a", "a", "a", "a", "a", "a")),//bin 1D coord: 11
          MercatorTimeTopicTestData(-99.0, 10, 101L, List("b", "b", "b", "b", "c", "a")),//bin 1D coord: 15
          MercatorTimeTopicTestData(95.0, -70.0, 101L, List("a", "a", "a", "a", "a", "a")))//bin 1D coord: 0

      val generatedData = sc.parallelize(testData).toDF()

      val topicsOp = MercatorTimeTopics(
        latCol,
        lonCol,
        timeCol,
        textCol,
        None,
        RangeDescription.fromCount(0, 800, 10),
        10,
        0 until 3,
        4
      )(_)

      val opsResult = topicsOp(generatedData)

      val coordsResult = opsResult.map(_.coords).collect().toSet
      val expectedCoords = Set(
        (0, 0, 0), // l0
        (1, 1, 0), (1, 0, 1), // l1
        (2, 3, 0), (2, 0, 2)) // l2
      assertResult((Set(), Set()))((expectedCoords diff coordsResult, coordsResult diff expectedCoords))

      val binValues = opsResult.map(elem => (elem.coords, elem.bins)).collect()
      val binCoords = binValues.map {
        elem => (elem._1, new String(intScoreListToByteArray(elem._2).toArray))
      }
      val selectedBinValue = binCoords.filter { input =>
        input._1 match { //extract some level two coordinates and their bin values
          case Tuple3(2,0,2) => true
          case _ => false
        }
      }

      //tile (2,0,2) contains two populated bins at binIndex 27 and 31
      //bin values at binIndex 31 are result of first and third MercatorTimeTopicTestData in testData
      val binValCheck = Array(
        (Tuple3(2,0,2), """[{"binIndex": 27, "topics": {"a": 6}},{"binIndex": 31, "topics": {"c": 5, "b": 4, "d": 2, "a": 1}}]""")
      )
      assertResult(binValCheck)(selectedBinValue)

    }
  }
}
