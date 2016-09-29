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



import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.projection.numeric.{CartesianProjection, MercatorProjection}
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.xdata.ops.salt.{CartesianOp, ZXYOp}



object TFIDFWordCloud extends ZXYOp {
  def cartesianTermFrequency(xCol: String,
                             yCol: String,
                             textCol: String,
                             bounds: (Double, Double, Double, Double),
                             zoomLevels: Seq[Int])
                            (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], Nothing]] = {
    // Pull out term and document frequencies for each tile
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    val binAggregator = new WordCounter

    val projection = new CartesianProjection(zoomLevels, (bounds._1, bounds._2), (bounds._3, bounds._4))

    super.apply(projection, 1, xCol, yCol, textCol, binAggregator, None)(request)(input)
  }

  def mercatorTermFrequency(xCol: String,
                            yCol: String,
                            textCol: String,
                            zoomLevels: Seq[Int])
                           (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], Nothing]] = {
    // Pull out term and document frequencies for each tile
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    val binAggregator = new WordCounter

    val projection = new MercatorProjection(zoomLevels)

    super.apply(projection, 1, xCol, yCol, textCol, binAggregator, None)(request)(input)
  }

  /**
    * Perform TFIDF on the output of termFrequency
    *
    * TODO: change the output to RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Double], Nothing]] once
    * salt allows non-package-private creation of SeriesData objects.
    *
    * @param numWords
    * @param input
    * @return
    */
  def doTFIDF (numWords: Int)(input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], Nothing]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], Nothing]] = {
    // Calculate number of documents and the inverse document frequency
    val (n, nD) = input.map{data =>
      // One bin per tile - get its data
      val level = data.coords._1
      val tileData = data.bins(0)

      // Doc count (by level)
      val docCount = Array.fill(level+1)(0)
      docCount(level) = 1
      // Count of docs containing each term (by level)
      val terms = Array.fill(level+1)(Map[String, Int]())
      terms(level) = tileData.map{case (term, frequency) => (term -> 1)}

      (docCount, terms)
    }.reduce { (a, b) =>
      val (docCountA, termsA) = a
      val (docCountB, termsB) = b

      val length = docCountA.length max docCountB.length

      val docCount = (docCountA.padTo(length, 0) zip docCountB.padTo(length, 0)).map{case (ac, bc) => ac + bc}
      val termsAA = termsA.padTo(length, Map[String, Int]())
      val termsBB = termsB.padTo(length, Map[String, Int]())

      val terms = (termsAA zip termsBB).map{case (ta, tb) =>
        (ta.keys ++ tb.keys).map(key => (key -> (ta.getOrElse(key, 0) + tb.getOrElse(key, 0)))).toMap
      }

      (docCount, terms)
    }

    input.map{data =>
      val tileCoordinate = data.coords
      // One bin per tile
      val binCoordinate = (1, 1)
      val termFrequencies = data.bins(0)
      val level = tileCoordinate._1
      val N = n(level).toDouble
      val termDocCounts = nD(level)

      val termScores = termFrequencies.map { case (term, frequencyInDoc) =>
        val tf = frequencyInDoc
        val idf = math.log(N / termDocCounts(term))
        term -> (tf * idf)
      }.toList.sortBy(-_._2).take(numWords)

      new SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], Nothing](
        data.projection,
        data.maxBin,
        tileCoordinate,
        new SparseArray[List[(String, Double)]](1, List[(String, Double)](), Map(0 -> termScores)),
        None
      )
    }
  }
}

object WordCounter {
  val wordSeparators = "('$|^'|'[^a-zA-Z_0-9']+|[^a-zA-Z_0-9']+'|[^a-zA-Z_0-9'])+"
}
class WordCounter extends Aggregator[String, MutableMap[String, Int], Map[String, Int]] {
  override def default(): MutableMap[String, Int] = MutableMap[String, Int]()

  override def finish(intermediate: MutableMap[String, Int]): Map[String, Int] = intermediate.toMap

  override def merge(left: MutableMap[String, Int], right: MutableMap[String, Int]): MutableMap[String, Int] = {
    right.foreach{case (term, frequency) =>
        left(term) = left.getOrElse(term, 0) + frequency
    }
    left
  }

  override def add(current: MutableMap[String, Int], next: Option[String]): MutableMap[String, Int] = {
    next.foreach{input =>
      input.split(WordCounter.wordSeparators).map(_.toLowerCase.trim).filter(!_.isEmpty).foreach(word =>
        current(word) = current.getOrElse(word, 0) + 1
      )
    }
    current
  }
}
