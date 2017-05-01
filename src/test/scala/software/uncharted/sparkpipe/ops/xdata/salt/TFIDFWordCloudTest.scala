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
import software.uncharted.salt.core.projection.numeric.CartesianProjection
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.salt.xdata.analytic.WordCounter
import software.uncharted.sparkpipe.ops.xdata.text.analytics.{DictionaryConfig, LogIDF, RawTF}
import software.uncharted.xdata.spark.SparkFunSpec
import software.uncharted.xdata.tiling.config.TFIDFConfig

import scala.collection.mutable.{Map => MutableMap}



class TFIDFWordCloudTest extends SparkFunSpec {
  private val tfidfConf = TFIDFConfig(
    RawTF, LogIDF, DictionaryConfig(true, None, None, None, None, None), 10
    )

  private def createTestData () = {
    // Note that TF:IDF is done separately on each level
    // So for each level, the document is the aggregation of all data that maps to a given tile
    //
    //                                       tile coordinates for each datum
    //                                       lvl 0      lvl 1      lvl 2
    val rddData = sc.parallelize(Seq(
      TFIDFData(1.0, 1.0, "aaa bbb fff"), // (0, 0, 0), (1, 0, 0), (2, 0, 0)
      TFIDFData(1.0, 3.0, "aaa bbb ggg"), // (0, 0, 0), (1, 0, 0), (2, 0, 1)
      TFIDFData(3.0, 1.0, "aaa bbb hhh"), // (0, 0, 0), (1, 0, 0), (2, 1, 0)
      TFIDFData(3.0, 3.0, "aaa bbb iii"), // (0, 0, 0), (1, 0, 0), (2, 1, 1)

      TFIDFData(1.0, 5.0, "aaa ccc fff"), // (0, 0, 0), (1, 0, 1), (2, 0, 2)
      TFIDFData(1.0, 7.0, "aaa ccc ggg"), // (0, 0, 0), (1, 0, 1), (2, 0, 3)
      TFIDFData(3.0, 5.0, "aaa ccc hhh"), // (0, 0, 0), (1, 0, 1), (2, 1, 2)
      TFIDFData(3.0, 7.0, "aaa ccc iii"), // (0, 0, 0), (1, 0, 1), (2, 1, 3)

      TFIDFData(5.0, 1.0, "aaa ddd fff"), // (0, 0, 0), (1, 1, 0), (2, 2, 0)
      TFIDFData(5.0, 3.0, "aaa ddd ggg"), // (0, 0, 0), (1, 1, 0), (2, 2, 1)
      TFIDFData(7.0, 1.0, "aaa ddd hhh"), // (0, 0, 0), (1, 1, 0), (2, 3, 0)
      TFIDFData(7.0, 3.0, "aaa ddd iii"), // (0, 0, 0), (1, 1, 0), (2, 3, 1)

      TFIDFData(5.0, 5.0, "aaa eee fff"), // (0, 0, 0), (1, 1, 1), (2, 2, 2)
      TFIDFData(5.0, 7.0, "aaa eee ggg"), // (0, 0, 0), (1, 1, 1), (2, 2, 3)
      TFIDFData(7.0, 5.0, "aaa eee hhh"), // (0, 0, 0), (1, 1, 1), (2, 3, 2)
      TFIDFData(7.0, 7.0, "aaa eee iii")  // (0, 0, 0), (1, 1, 1), (2, 3, 3)
    ))
    sparkSession.createDataFrame(rddData)
  }

  describe("Tile transformation") {
    it("should transform sparse arrays correctly") {
      val base = SparseArray(12, 0)(2 -> 4, 3 -> 9, 7 -> 49)
      val transformed = base.map(n => n-1)
      assert(transformed.isInstanceOf[SparseArray[Int]])

      val output = transformed.asInstanceOf[SparseArray[Int]]
      assert(output(2) === 3)
      assert(output(3) === 8)
      assert(output(7) == 48)
      assert(output.density() === 0.25)
    }
  }

  describe("TF/IDF Word Cloud Test") {
    it("should count words correctly") {
      val input =
        """Not, I'll not, Carrion Comfort, despair, not feast on thee;
          |Not untwist - slack they may be - these last strands of man
          |In me or, most weary, cry, 'I can no more.'  I can;
          |Can something, hope, wish day cope, not choose not to be.
          |But ah, but O thou terrible, why wouldst thou rude on me
          |Thy wring-world right foot rock? lay a lionlimb against me? scan
          |With darksome devouring eyes my bruised bones? and fan,
          |O in turns of tempest, me heaped there; me frantic to avoid thee and flee?
          |
          |Why? That my chaff my fly; my grain lie, sheer and clear;
          |Nay in all that toil, that coil, since (seems) I kissed the rod,
          |Hand rather, my heart lo! lapped strength, stole joy, would laught, cheer.
          |Cheer whom though? the hero whose heaven-handling flung me, foot trod
          |Me? or me that fought him? O which one? is it each one? That night, that year
          |Of now done darkness I wretch lay wrestling with (my God!) my God.""".stripMargin

      val wc = new WordCounter
      val result = wc.add(MutableMap[String, Int](), Some(input))
      result.map{case (word, frequency) =>
        assert(!word.contains(" "))
        assert(!word.contains(","))
        assert(!word.contains(";"))
        assert(!word.contains("-"))
        assert(!word.contains("\""))
        assert(!word.contains("?"))
        assert(!word.startsWith("'"))
        assert(!word.endsWith("'"))
      }
      assert(8 === result("me"))
      assert(7 === result("my"))
      assert(1 === result("carrion"))
      assert(1 === result("comfort"))
    }

    it("should calculate tf*idf correctly using the fast calculation") {
      val data = createTestData()

      val projection = new CartesianProjection(Seq(0, 1, 2), (0.0, 0.0), (8.0, 8.0))
      val termFrequencies = TileTextOperations.termFrequencyOp("x", "y", "text", projection, Seq(0, 1, 2))(data)

      val tfidf = TileTextOperations.doTFIDFByTileFast[Nothing](tfidfConf)(termFrequencies).collect.sortBy { r =>
        16 * r.coords._1 + 4 * r.coords._2 + r.coords._3
      }.map { r =>
        (r.coords, r.bins(0).toMap)
      }.toMap

      // Test tile level 0
      // There is only one tile (i.e., document) on this level, so all scores should be 0
      assert(tfidf((0, 0, 0)).map(_._2).reduce(_ + _) === 0.0)

      // Test tile level 1
      // Make sure "aaa" has no distinguishing score - it's equally present everywhere.
      assert(tfidf((1, 0, 0))("aaa") === 0.0)
      assert(tfidf((1, 1, 0))("aaa") === 0.0)
      assert(tfidf((1, 0, 1))("aaa") === 0.0)
      assert(tfidf((1, 1, 1))("aaa") === 0.0)

      // For each of the other level 1 tiles, it should have one other word unique to it (according to its position)
      // from "bbb" to "eee".
      // Make sure that one other word is present, and has the maximum score on that tile
      // Also, the strings "fff" through "iii" should be equally present on each tile on this level, so should be
      // present, but with 0 score
      val tile1_00 = tfidf((1, 0, 0))
      assert(tile1_00("bbb") === tile1_00.map(_._2).max)
      Seq("ccc", "ddd", "eee").foreach(key => assert(!tile1_00.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_00(key)))

      val tile1_01 = tfidf((1, 0, 1))
      assert(tile1_01("ccc") === tile1_01.map(_._2).max)
      Seq("bbb", "ddd", "eee").foreach(key => assert(!tile1_01.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_01(key)))

      val tile1_10 = tfidf((1, 1, 0))
      assert(tile1_10("ddd") === tile1_10.map(_._2).max)
      Seq("bbb", "ccc", "eee").foreach(key => assert(!tile1_10.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_10(key)))

      val tile1_11 = tfidf((1, 1, 1))
      assert(tile1_11("eee") === tile1_11.map(_._2).max)
      Seq("bbb", "ccc", "ddd").foreach(key => assert(!tile1_11.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_11(key)))

      // We could also test level 2 tiles... leaving that as an exercise for later for now.
    }


    it("should calculate tf*idf correctly using the slow calculation") {
      val data = createTestData()

      val projection = new CartesianProjection(Seq(0, 1, 2), (0.0, 0.0), (8.0, 8.0))
      val termFrequencies = TileTextOperations.termFrequencyOp("x", "y", "text", projection, Seq(0, 1, 2))(data)

      val tfidf = TileTextOperations.doTFIDFByTileSlow[Nothing](tfidfConf)(termFrequencies).collect.sortBy { r =>
        16 * r.coords._1 + 4 * r.coords._2 + r.coords._3
      }.map { r =>
        (r.coords, r.bins(0).toMap)
      }.toMap

      // Test tile level 0
      // There is only one tile (i.e., document) on this level, so all scores should be 0
      assert(tfidf((0, 0, 0)).map(_._2).reduce(_ + _) === 0.0)

      // Test tile level 1
      // Make sure "aaa" has no distinguishing score - it's equally present everywhere.
      assert(tfidf((1, 0, 0))("aaa") === 0.0)
      assert(tfidf((1, 1, 0))("aaa") === 0.0)
      assert(tfidf((1, 0, 1))("aaa") === 0.0)
      assert(tfidf((1, 1, 1))("aaa") === 0.0)

      // For each of the other level 1 tiles, it should have one other word unique to it (according to its position)
      // from "bbb" to "eee".
      // Make sure that one other word is present, and has the maximum score on that tile
      // Also, the strings "fff" through "iii" should be equally present on each tile on this level, so should be
      // present, but with 0 score
      val tile1_00 = tfidf((1, 0, 0))
      assert(tile1_00("bbb") === tile1_00.map(_._2).max)
      Seq("ccc", "ddd", "eee").foreach(key => assert(!tile1_00.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_00(key)))

      val tile1_01 = tfidf((1, 0, 1))
      assert(tile1_01("ccc") === tile1_01.map(_._2).max)
      Seq("bbb", "ddd", "eee").foreach(key => assert(!tile1_01.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_01(key)))

      val tile1_10 = tfidf((1, 1, 0))
      assert(tile1_10("ddd") === tile1_10.map(_._2).max)
      Seq("bbb", "ccc", "eee").foreach(key => assert(!tile1_10.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_10(key)))

      val tile1_11 = tfidf((1, 1, 1))
      assert(tile1_11("eee") === tile1_11.map(_._2).max)
      Seq("bbb", "ccc", "ddd").foreach(key => assert(!tile1_11.contains(key)))
      Seq("fff", "ggg", "hhh", "iii").foreach(key => assert(0.0 === tile1_11(key)))

      // We could also test level 2 tiles... leaving that as an exercise for later for now.
    }
  }

  describe("timing trials") {
    def runNewFast (data: DataFrame): Unit = {
      val projection = new CartesianProjection(Seq(0, 1, 2), (0.0, 0.0), (8.0, 8.0))
      val termFrequencies = TileTextOperations.termFrequencyOp("x", "y", "text", projection, Seq(0, 1, 2))(data)

      val tfidf = TileTextOperations.doTFIDFByTileFast[Nothing](tfidfConf)(termFrequencies).collect
    }
    def runNewSlow (data: DataFrame): Unit = {
      val projection = new CartesianProjection(Seq(0, 1, 2), (0.0, 0.0), (8.0, 8.0))
      val termFrequencies = TileTextOperations.termFrequencyOp("x", "y", "text", projection, Seq(0, 1, 2))(data)

      val tfidf = TileTextOperations.doTFIDFByTileSlow[Nothing](tfidfConf)(termFrequencies).collect
    }
    def time[T] (f: => T): (T, Long) = {
      val startTime = System.currentTimeMillis()
      val result: T = f
      val endTime = System.currentTimeMillis()
      (result, (endTime - startTime))
    }

    ignore("testing relative speeds of various algorithms") {
      var tnf = 0L
      var tns = 0L
      var to = 0L
      val N = 1000

      for (i <- 1 to N) {
        {
          val (result, t) = time(runNewFast(createTestData()))
          tnf += t
        }
        {
          val (result, t) = time(runNewSlow(createTestData()))
          tns += t
        }
      }

      println(s"Timing stats for $N runs:")
      println(s"new fast tfidf implementation: $tnf")
      println(s"new slow tfidf implementation: $tns")
    }
  }
}

case class TFIDFData (x: Double, y: Double, text: String)
