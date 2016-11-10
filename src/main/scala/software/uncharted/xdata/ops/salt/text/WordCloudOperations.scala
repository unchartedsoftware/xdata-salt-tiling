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
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.xdata.ops.salt.ZXYOp
import software.uncharted.xdata.sparkpipe.config.LDAConfig

import scala.reflect.ClassTag


object WordCloudOperations extends ZXYOp {
  def termFrequencyOp(xCol: String,
                      yCol: String,
                      textCol: String,
                      projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
                      zoomLevels: Seq[Int])
                     (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], Nothing]] = {
    // Pull out term and document frequencies for each tile
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    val binAggregator = new WordCounter

    super.apply(projection, 1, xCol, yCol, textCol, binAggregator, None)(request)(input)
  }

  /**
    * Take a set of tiles, and transform the tile contents
    *
    * @param input The input data set of already tiled data
    * @tparam TC The tile coordinate type
    * @tparam BC The bin coordinate type
    * @tparam X The aggregators' coordinate type
    * @tparam VI The input value type
    * @tparam VO The output value type
    * @return A new set of tiles with the contents transformed as per the input function
    */
  def tileTransformationOp[TC, BC, X, VI, VO: ClassTag] (transformation: (VI, TC, BC) => VO)
                                                        (input: RDD[SeriesData[TC, BC, VI, X]]): RDD[SeriesData[TC, BC, VO, X]] = {
    input.map { data =>
      val tileCoordinate = data.coords
      val projection = data.projection
      val maxBin = data.maxBin

      val outputBins = data.bins.mapWithIndex { (value, index) =>
        val binCoordinate = projection.binFrom1D(index, maxBin)
        transformation(value, tileCoordinate, binCoordinate)
      }

      new SeriesData[TC, BC, VO, X](
        data.projection, maxBin, tileCoordinate, outputBins, data.tileMeta
      )
    }
  }

  /**
    * Perform TFIDF on the output of termFrequency, on a tile by tile basis
    *
    * This assumes a single bin per tile
    *
    * @param numWords
    * @param input
    * @return
    */
  def doTFIDFByTile[X](numWords: Int)(input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]] = {
    // Calculate number of documents and the inverse document frequency
    val levelInfos = input.map { data =>
      // One bin per tile - get its data
      val level = data.coords._1
      val tileData = data.bins(0)

      // Doc count (by level)
      val docCount = oneTermArray(level + 1, 0, level, 1)
      // Count of docs containing each term (by level)
      val terms = oneTermArray(level + 1, Map[String, Int](), level, tileData.map { case (term, frequency) => (term -> 1) })

      TFIDFInfo(docCount, terms)
    }.reduce { (a, b) => a + b }

    def transformBin (termFrequencies: Map[String, Int], tileCoordinate: (Int, Int, Int), binCoordinate: (Int, Int)): List[(String, Double)] = {
      val level = tileCoordinate._1
      val N = levelInfos.docCount(level).toDouble
      val termDocCounts = levelInfos.docTerms(level)
      termFrequencies.map { case (term, frequencyInDoc) =>
        val tf = frequencyInDoc
        val idf = math.log(N / termDocCounts(term))
        term -> (tf * idf)
      }.toList.sortBy(-_._2).take(numWords)
    }
    tileTransformationOp(transformBin)(input)
  }

  /**
    * Perform LDA on the output of termFrequency, on a tile by tile basis, outputting the top topics in each tile
    *
    * This assumes a single bin per tile
    *
    * @param config The configuration for how to run LDA
    * @param input The input data of tiles of word bags
    * @tparam X The type of metadata associated with each tile
    * @return A new tile set containing the LDA results on each word bag
    */
  def ldaTopicsByTile[X] (config: LDAConfig)
                         (input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]] = {
    type InSeries  = SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]
    type OutSeries = SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]
    val transform: InSeries => Map[String, Int] = _.bins(0)

    LDAOp.wordBagLDA(config, transform)(input).map { case (inData, ldaResults) =>
      val outputResults = ldaResults.map { t =>
        (t.topic.map { ws => ws.word + config.scoreSeparator + ws.score }.mkString(config.wordSeparator), t.score)
      }.toList
      new OutSeries(
        inData.projection,
        inData.maxBin,
        inData.coords,
        SparseArray[List[(String, Double)]](1, List[(String, Double)](), 0.0f)(0 -> outputResults),
        inData.tileMeta)
    }
  }

  /**
    * Perform LDA on the output of termFrequency, on a tile by tile basis, outputting the top words in each tile,
    * weighted by topic weight and word-within-topic weight
    *
    * This assumes a single bin per tile
    *
    * @param config The configuration for how to run LDA
    * @param input The input data of tiles of word bags
    * @tparam X The type of metadata associated with each tile
    * @return A new tile set containing the LDA results on each word bag
    */
  def ldaWordsByTile[X] (config: LDAConfig)
                        (input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]] = {
    type InSeries  = SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]
    type OutSeries = SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]
    val transform: InSeries => Map[String, Int] = _.bins(0)

    LDAOp.wordBagLDA(config, transform)(input).map { case (inData, ldaResults) =>
      val wordScores = MutableMap[String, Double]()
      ldaResults.foreach { t =>
        val topicScore = t.score
        t.topic.foreach { ws =>
          wordScores(ws.word) = wordScores.getOrElse(ws.word, 0.0) + topicScore * ws.score
        }
      }
      val outputResults = wordScores.toList.sortBy(-_._2).take(config.wordsPerTopic)
      new OutSeries(
        inData.projection,
        inData.maxBin,
        inData.coords,
        SparseArray[List[(String, Double)]](1, List[(String, Double)](), 0.0f)(0 -> outputResults),
        inData.tileMeta)
    }
  }

  private def oneTermArray[T: ClassTag](size: Int, defaultValue: T, occupiedIndex: Int, occupiedValue: T): Array[T] = {
    val result = Array.fill[T](size)(defaultValue)
    result(occupiedIndex) = occupiedValue
    result
  }
}

/**
  * A count of documents, and of the number of documents in which each term occurs, by tiling level
  *
  * @param docCount The number of documents, by tiling level
  * @param docTerms The number of documents in which each term occurs, by tiling level
  */
case class TFIDFInfo(docCount: Array[Int], docTerms: Array[Map[String, Int]]) {
  // scalastyle:off method.name
  def +(that: TFIDFInfo): TFIDFInfo = {
    val length = this.docCount.length max that.docCount.length

    val aggregateCount = (this.docCount.padTo(length, 0) zip that.docCount.padTo(length, 0)).map { case (ac, bc) => ac + bc }
    val termsAA = this.docTerms.padTo(length, Map[String, Int]())
    val termsBB = that.docTerms.padTo(length, Map[String, Int]())

    val aggregateTerms = (termsAA zip termsBB).map { case (ta, tb) =>
      (ta.keys ++ tb.keys).map(key => (key -> (ta.getOrElse(key, 0) + tb.getOrElse(key, 0)))).toMap
    }

    TFIDFInfo(aggregateCount, aggregateTerms)
  }

  // scalastyle:on method.name
}

object WordCounter {
  val wordSeparators = "('$|^'|'[^a-zA-Z_0-9']+|[^a-zA-Z_0-9']+'|[^a-zA-Z_0-9'])+"
}

class WordCounter extends Aggregator[String, MutableMap[String, Int], Map[String, Int]] {
  override def default(): MutableMap[String, Int] = MutableMap[String, Int]()

  override def finish(intermediate: MutableMap[String, Int]): Map[String, Int] = intermediate.toMap

  override def merge(left: MutableMap[String, Int], right: MutableMap[String, Int]): MutableMap[String, Int] = {
    right.foreach { case (term, frequency) =>
      left(term) = left.getOrElse(term, 0) + frequency
    }
    left
  }

  override def add(current: MutableMap[String, Int], next: Option[String]): MutableMap[String, Int] = {
    next.foreach { input =>
      input.split(WordCounter.wordSeparators).map(_.toLowerCase.trim).filter(!_.isEmpty).foreach(word =>
        current(word) = current.getOrElse(word, 0) + 1
      )
    }
    current
  }
}
