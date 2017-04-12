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

package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.sparkpipe.ops.xdata.text.DictionaryConfiguration
import software.uncharted.xdata.tiling.config.LDAConfig
import software.uncharted.sparkpipe.ops.xdata.text.LDAOp.wordBagLDA
import scala.collection.mutable

object LDATopicsOp {
  val tmpDir: String = "/tmp"

  /**
    * Perform LDA on the output of termFrequency, on a tile by tile basis, outputting the top topics in each tile
    *
    * This assumes a single bin per tile
    *
    * @param dictionaryConfig The dictionary creation configuration
    * @param ldaConfig The configuration for how to run LDA
    * @param input The input data of tiles of word bags
    * @tparam X The type of metadata associated with each tile
    * @return A new tile set containing the LDA results on each word bag
    */
  def ldaTopicsByTile[X] (dictionaryConfig: DictionaryConfiguration, ldaConfig: LDAConfig)
                         (input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]] = {
    type InSeries  = SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]
    type OutSeries = SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]
    val transform: InSeries => Map[String, Int] = _.bins(0)

    wordBagLDA(dictionaryConfig, ldaConfig, transform)(input).map { case (inData, ldaResults) =>
      val outputResults = ldaResults.map { t =>
        (t.topic.map { ws => ws.word + ldaConfig.scoreSeparator + ws.score }.mkString(ldaConfig.wordSeparator), t.score)
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
    * @param dictionaryConfig The dictionary creation configuration
    * @param ldaConfig The configuration for how to run LDA
    * @param input The input data of tiles of word bags
    * @tparam X The type of metadata associated with each tile
    * @return A new tile set containing the LDA results on each word bag
    */
  def ldaWordsByTile[X] (dictionaryConfig: DictionaryConfiguration, ldaConfig: LDAConfig)
                        (input: RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]]):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]] = {
    type InSeries  = SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], X]
    type OutSeries = SeriesData[(Int, Int, Int), (Int, Int), List[(String, Double)], X]
    val transform: InSeries => Map[String, Int] = _.bins(0)

    wordBagLDA(dictionaryConfig, ldaConfig, transform)(input).map { case (inData, ldaResults) =>
      val wordScores = mutable.HashMap[String, Double]()
      ldaResults.foreach { t =>
        val topicScore = t.score
        t.topic.foreach { ws =>
          wordScores(ws.word) = wordScores.getOrElse(ws.word, 0.0) + topicScore * ws.score
        }
      }
      val outputResults = wordScores.toList.sortBy(-_._2).take(ldaConfig.wordsPerTopic)
      new OutSeries(
        inData.projection,
        inData.maxBin,
        inData.coords,
        SparseArray[List[(String, Double)]](1, List[(String, Double)](), 0.0f)(0 -> outputResults),
        inData.tileMeta)
    }
  }
}
