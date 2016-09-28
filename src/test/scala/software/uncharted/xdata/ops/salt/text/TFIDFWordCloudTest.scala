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
package software.uncharted.xdata.ops.salt.text

import software.uncharted.xdata.spark.SparkFunSpec

import scala.collection.mutable.{Map => MutableMap}
import org.scalatest.FunSuite

class TFIDFWordCloudTest extends SparkFunSpec {
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

    it("should calculate tf/idf correctly") {
      val rddData = sc.parallelize(Seq(
        TFIDFData(1.0, 1.0, "aaa bbb fff"),
        TFIDFData(1.0, 3.0, "aaa bbb ggg"),
        TFIDFData(3.0, 1.0, "aaa bbb hhh"),
        TFIDFData(3.0, 3.0, "aaa bbb iii"),

        TFIDFData(5.0, 1.0, "aaa ccc fff"),
        TFIDFData(5.0, 3.0, "aaa ccc ggg"),
        TFIDFData(7.0, 1.0, "aaa ccc hhh"),
        TFIDFData(7.0, 3.0, "aaa ccc iii"),

        TFIDFData(1.0, 5.0, "aaa ddd fff"),
        TFIDFData(1.0, 7.0, "aaa ddd ggg"),
        TFIDFData(3.0, 5.0, "aaa ddd hhh"),
        TFIDFData(3.0, 7.0, "aaa ddd iii"),

        TFIDFData(5.0, 5.0, "aaa eee fff"),
        TFIDFData(5.0, 7.0, "aaa eee ggg"),
        TFIDFData(7.0, 5.0, "aaa eee hhh"),
        TFIDFData(7.0, 7.0, "aaa eee iii")
      ))
      val data = sqlc.createDataFrame(rddData)

      val termFrequencies = TFIDFWordCloud.termFrequency("x", "y", "text", Some((0.0, 0.0, 8.0, 8.0)), Seq(0, 1, 2))(data)

      val tfidf = TFIDFWordCloud.doTFIDF(5)(termFrequencies).collect.sortBy(r => 16*r._1._1 + 4*r._1._2 + r._1._3).map { r =>
        (r._1, r._3)
      }.toMap
      assert(tfidf((0, 0, 0)).map(_._2).reduce(_ + _) === 0.0)
      assert(tfidf((1, 0, 0))("aaa") === 0.0)
      assert(tfidf((1, 1, 0))("aaa") === 0.0)
      assert(tfidf((1, 0, 1))("aaa") === 0.0)
      assert(tfidf((1, 1, 1))("aaa") === 0.0)

      assert(tfidf((1, 0, 0))("bbb") === tfidf((1, 0, 0)).map(_._2).max)
      assert(!tfidf((1, 0, 0)).contains("ccc"))
      assert(!tfidf((1, 0, 0)).contains("ddd"))
      assert(!tfidf((1, 0, 0)).contains("eee"))

      assert(!tfidf((1, 1, 0)).contains("bbb"))
      assert(tfidf((1, 1, 0))("ccc") === tfidf((1, 0, 0)).map(_._2).max)
      assert(!tfidf((1, 1, 0)).contains("ddd"))
      assert(!tfidf((1, 1, 0)).contains("eee"))

      assert(!tfidf((1, 0, 1)).contains("bbb"))
      assert(!tfidf((1, 0, 1)).contains("ccc"))
      assert(tfidf((1, 0, 1))("ddd") === tfidf((1, 0, 0)).map(_._2).max)
      assert(!tfidf((1, 0, 1)).contains("eee"))

      assert(!tfidf((1, 1, 1)).contains("bbb"))
      assert(!tfidf((1, 1, 1)).contains("ccc"))
      assert(!tfidf((1, 1, 1)).contains("ddd"))
      assert(tfidf((1, 1, 1))("eee") === tfidf((1, 0, 0)).map(_._2).max)
    }
  }
}

case class TFIDFData (x: Double, y: Double, text: String)
