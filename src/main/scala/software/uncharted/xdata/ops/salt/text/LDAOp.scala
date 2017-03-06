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

import org.apache.spark.SparkContext
import software.uncharted.xdata.spark.mllib.MatrixUtilities

import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.xdata.sparkpipe.config.LDAConfig

/**
  * An operation to run Latent Dirichlet Allocation on texts in a corpus
  */
object LDAOp {
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
      val wordScores = MutableMap[String, Double]()
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
  /**
    * Perform LDA analysis on documents in a dataframe
    *
    * @param idCol The name of a column containing a (long) id unique to each row
    * @param textCol The name of the column containing the text to analyze
    * @param dictionaryConfig The dictionary creation configuration
    * @param ldaConfig LDA job configuration
    * @param input The dataframe containing the data to analyze
    * @return A dataframe containing the input data, augmented with the topics found in that input data
    */
  def textLDA (idCol: String, textCol: String, dictionaryConfig: DictionaryConfiguration, ldaConfig: LDAConfig)
              (input: DataFrame): DataFrame = {
    val sqlc = input.sqlContext

    // Mutate our input into indexed word bags
    val inputWords = TextOperations.textToWordBags[Row, (Long, Map[String, Int])](
      dictionaryConfig,
      _.getString(1),
      (row, wordBag) => (row.getLong(0), wordBag)
    )(input.select(idCol, textCol).rdd)

    // Create our dictionary from the set of input words
    val dictionary = TextOperations.getDictionary[(Long, Map[String, Int])](dictionaryConfig, _._2)(inputWords)
      .zipWithIndex.map { case ((word, count), index) => (word, index)}.toMap

    // Perform our LDA analysis
    val rawResults = wordBagLDATopics(ldaConfig, dictionary, inputWords)

    // Mutate to dataframe form for joining with original data
    val dfResults = sqlc.createDataFrame(rawResults.map{case (index, scores) => DocumentTopics(index, scores)})

    // Join our LDA results back with our original data
    input.join(dfResults, input(idCol) === dfResults("ldaDocumentIndex")).drop(new Column("ldaDocumentIndex"))
  }

  /**
    * Perform LDA on an arbitrary dataset of unindexed documents
    *
    * @param dictionaryConfig The dictionary creation configuration
    * @param ldaConfig LDA job configuration
    * @param docExtractor A function to extract the document to be analyzed from each input record
    * @param input An RDD containing the data to analyze
    * @tparam T The input data type
    * @return A dataframe containing the input data, augmented with the topics found in that input data
    */
  def textLDA[T] (dictionaryConfig: DictionaryConfiguration, ldaConfig: LDAConfig, docExtractor: T => String)
                 (input: RDD[T]): RDD[(T, Seq[TopicScore])] = {
    val indexedInput = input.map(t => (t, docExtractor(t))).zipWithIndex().map(_.swap)

    // Mutate our input into indexed word bags
    val inputWords = TextOperations.textToWordBags[(Long, (T, String)), (Long, (T, Map[String, Int]))](
      dictionaryConfig,
      _._2._2,
      (original, wordBag) => (original._1, (original._2._1, wordBag))
    )(indexedInput)

    // Create our dictionary from the set of input words
    val dictionary = TextOperations.getDictionary[(Long, (T, Map[String, Int]))](dictionaryConfig, _._2._2)(inputWords)
      .zipWithIndex.map { case ((word, count), index) => (word, index)}.toMap

    // Perform our LDA analysis
    val ldaResults = wordBagLDATopics(ldaConfig, dictionary, inputWords.map { case (index, (original, words)) => (index, words) })

    // Join our LDA results back with our original data
    indexedInput.join(ldaResults).map { case (index, ((original, words), scores)) =>
      (original, scores)
    }
  }

  /**
    * Perform LDA on an arbitrary dataset of indexed documents.
    *
    * @param dictionaryConfig The dictionary creation configuration
    * @param ldaConfig LDA job configuration
    * @param docExtractor A function to extract the document to be analyzed from each input record
    * @param idExtractor A function to extract the document index from each input record.  Each record should have a
    *                    unique document index.
    * @param input An RDD containing the data to analyze
    * @tparam T The input data type
    * @return
    */
  def textLDATopics[T] (dictionaryConfig: DictionaryConfiguration, ldaConfig:LDAConfig, docExtractor: T => String, idExtractor: T => Long)
                       (input: RDD[T]): RDD[(Long, Seq[TopicScore])] = {
    val indexedDocuments = input.map(t => (idExtractor(t), docExtractor(t)))

    // Mutate our input into indexed word bags
    val inputWords = TextOperations.textToWordBags[(Long, String), (Long, Map[String, Int])](
      dictionaryConfig,
      _._2,
      (original, wordBag) => (original._1, wordBag)
    )(indexedDocuments)

    // Create our dictionary from the set of input words
    val dictionary = TextOperations.getDictionary[(Long, Map[String, Int])](dictionaryConfig, _._2)(inputWords)
      .zipWithIndex.map { case ((word, count), index) => (word, index)}.toMap

    // Perform our LDA analysis
    wordBagLDATopics(ldaConfig, dictionary, inputWords)
  }


  /**
    * Perform LDA on an arbitrary dataset of unindexed word bags
    */
  def wordBagLDA[T] (dictionaryConfig: DictionaryConfiguration, ldaConfig: LDAConfig, wordBagExtractor: T => Map[String, Int])
                    (input: RDD[T]): RDD[(T, Seq[TopicScore])] = {
    val dictionary = TextOperations.getDictionary(dictionaryConfig, wordBagExtractor)(input)
      .zipWithIndex.map { case ((word, count), index) => (word, index)}.toMap

    val wordBagsWithIds = input.map(t => (t, wordBagExtractor(t))).zipWithIndex().map(_.swap)
    val ldaResults = wordBagLDATopics(ldaConfig, dictionary, wordBagsWithIds.map { case (key, (in, wordBag)) => (key, wordBag) })

    wordBagsWithIds.join(ldaResults).map { case (key, ((in, wordBag), scores)) =>
      (in, scores)
    }
  }

  /**
    * Perform LDA on an RDD of indexed documents.
    *
    * @param config LDA job configuration
    * @param dictionary A dictionary of words to consider in our documents
    * @param input An RDD of indexed word bags; the Long id field should be unique for each row.
    * @return An RDD of the same word bags, with a sequence of topics attached.  The third, attached, entry in each
    *         row should be read as Seq[(topic, topicScoreForDocument)], where the topic is
    *         Seq[(word, wordScoreForTopic)]
    */
  def wordBagLDATopics (config: LDAConfig, dictionary: Map[String, Int], input: RDD[(Long, Map[String, Int])]): RDD[(Long, Seq[TopicScore])] = {
    val documents = TextOperations.wordBagToWordVector(dictionary)(input)
    lda(config, dictionary, documents)
  }

  private def lda (config: LDAConfig,
                   dictionary: Map[String, Int],
                   documents: RDD[(Long, Vector)]): RDD[(Long, Seq[TopicScore])] = {
    val sc = documents.context

    val ldaEngine = new LDA()
      .setK(config.numTopics)
      .setOptimizer("em")
    config.chkptInterval.map(r => ldaEngine.setCheckpointInterval(r))
    config.maxIterations.map(r => ldaEngine.setMaxIterations(r))

    val model = getDistributedModel(sc, ldaEngine.run(documents))

    // Unwind the topics matrix using our dictionary (but reversed)
    val allTopics = getTopics(model, dictionary, config.wordsPerTopic)

    // Doc id, topics, weights
    val topicsByDocument: RDD[(Long, Array[Int], Array[Double])] = model.topTopicsPerDocument(config.topicsPerDocument)

    // Unwind the topics for each document
    topicsByDocument.map { case (docId, topics, weights) =>
      // Get the top n topic indices
      val topTopics = topics.zip(weights).sortBy(-_._2).take(config.topicsPerDocument).toSeq

      // Expand these into their topic word vectors
      (docId, topTopics.map { case (index, score) =>
        TopicScore(allTopics(index), score)
      })
    }
  }

  private def getDistributedModel (sc: SparkContext, model: LDAModel): DistributedLDAModel = {
    model match {
      case distrModel: DistributedLDAModel => distrModel
      case localModel: LocalLDAModel =>
        localModel.save(sc, tmpDir + "lda")
        DistributedLDAModel.load(sc, tmpDir + "lda")
    }
  }

  private def getTopics (model: DistributedLDAModel, dictionary: Map[String, Int], wordsPerTopic: Int): Map[Int, Seq[WordScore]] = {
    val topics: Matrix = model.topicsMatrix
    val reverseDictionary = dictionary.map(_.swap)

    (0 until topics.numCols).map(c => (c, MatrixUtilities.column(topics, c))).map { case (topicIndex, topicVector) =>
      val wordScores = (topicVector match {
        case v: DenseVector =>
          v.values.zipWithIndex.map { case (value, index) =>
            WordScore(reverseDictionary(index), value)
          }
        case v: SparseVector =>
          v.indices.map(reverseDictionary(_)).zip(v.values).map{case (word, score) => WordScore(word, score)}
      }).sortBy(-_.score).take(wordsPerTopic).toSeq

      (topicIndex, wordScores)
    }.toMap
  }
}

case class WordScore (word: String, score: Double)
case class TopicScore (topic: Seq[WordScore], score: Double)
case class DocumentTopics (ldaDocumentIndex: Long, topics: Seq[TopicScore])