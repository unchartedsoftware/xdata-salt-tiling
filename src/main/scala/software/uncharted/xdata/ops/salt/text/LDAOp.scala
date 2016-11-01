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
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import software.uncharted.xdata.sparkpipe.config.LDAConfig

/**
  * An operation to run Latent Dirichlet Allocation on texts in a corpus
  */
object LDAOp {
  val stopWords = {
    val stopWordsFile = "/software/uncharted/xdata/ops/salt/text/STOPWORDS/stopwords_en.txt"
    val stopWordsFileStream = getClass.getResourceAsStream(stopWordsFile)
    scala.io.Source.fromInputStream(stopWordsFileStream).getLines.map(_.trim.toLowerCase).toSet
  }
  val notWord = "('[^a-zA-Z]|[^a-zA-Z]'|[^a-zA-Z'])+"
  val tmpDir: String = "/tmp"

  /**
    * Take an RDD of (id, text) pairs, and transform the texts into vector references into a common dictionary, with the
    * entries being the count of words in that text.
    *
    * @param input A dataset of (id, text)
    * @tparam T The type of the ID associated with each text
    * @return a dictionary, and a dataset of (id, vector), where the vectors reference the words in the
    *         returned dictionary
    */
  def textToWordCount[T] (input: RDD[(T, String)]): (Map[String, Int], RDD[(T, Vector)]) = {
    // Break documents up into word bags, with counts, ignoring stop words
    val wordLists = input.map{case (id, text) =>
        val words = text.split(notWord).map(_.toLowerCase).filter(word => !stopWords.contains(word))
        val wordCounts = MutableMap[String, Int]()
        words.foreach(word => wordCounts(word) = wordCounts.getOrElse(word, 0) + 1)
      (id, wordCounts.toList.sorted.toArray)
    }
    // Get a list of all used words, in alphabetical order.
    val allWords = wordLists.flatMap(_._2).reduceByKey(_ + _).collect
    val dictionary = allWords.map(_._1).sorted.zipWithIndex.toMap

    // Port that dictionary back into our word maps, creating sparse vectors by map index
    val wordVectors = wordLists.map{case (id, wordList) =>
      val indicesAndValues = wordList.map { case (word, count) =>
        (dictionary(word), count)
      }.sortBy(_._1)
      val wordVector: Vector = new SparseVector(dictionary.size, indicesAndValues.map(_._1), indicesAndValues.map(_._2.toDouble))
      (id, wordVector)
    }
    (dictionary, wordVectors)
  }

  /**
    * Take an RDD of (id, word bag) pairs, and transform the word bags into vector references into a common dictionary,
    * with the entries being the count of words in each word bag.
    *
    * @param input A dataset of (id, word bag)
    * @tparam T The type of the ID associated with each word bag
    * @return a dictionary, and a dataset of (id, vector), where the vectors reference the words in the
    *         returned dictionary
    */
  def wordBagToWordCount[T] (input: RDD[(T, Map[String, Int])]): (Map[String, Int], RDD[(T, Vector)]) = {
    // Get a list of all used words, in alphabetical order - i.e., our dictionary
    val allWords = input.flatMap(_._2).reduceByKey(_ + _).collect
    val dictionary = allWords.map(_._1).sorted.zipWithIndex.toMap

    // Port that dictionary back into our word maps, creating sparse vectors by map index
    val wordVectors = input.map { case (id, wordBag) =>
      val indicesAndValues = wordBag.map { case (word, count) =>
        (dictionary(word), count)
      }.toArray.sortBy(_._1)
      val wordVector: Vector = new SparseVector(dictionary.size, indicesAndValues.map(_._1), indicesAndValues.map(_._2.toDouble))
      (id, wordVector)
    }
    (dictionary, wordVectors)
  }

  /**
    * Perform LDA analysis on documents in a dataframe
    *
    * @param idCol The name of a column containing a (long) id unique to each row
    * @param textCol The name of the column containing the text to analyze
    * @param config LDA job configuration
    * @param input The dataframe containing the data to analyze
    * @return The topics for each document
    */
  def textLDA (idCol: String, textCol: String, config: LDAConfig)(input: DataFrame): DataFrame = {
    val sqlc = input.sqlContext

    // Get the indexed documents
    val textRDD = input.select(idCol, textCol).rdd.map { row =>
      val id = row.getLong(0)
      val text = row.getString(1)
      (id, text)
    }

    // Perform our LDA analysis
    val rawResults = textLDA(config, textRDD)
    // Mutate to dataframe form for joining with original data
    val dfResults = sqlc.createDataFrame(rawResults.map{case (index, scores) => DocumentTopics(index, scores)})

    input.join(dfResults, input(idCol) === dfResults("ldaDocumentIndex")).drop(new Column("ldaDocumentIndex"))
  }

  /**
    * Perform LDA on an arbitrary dataset of unindexed documents
    */
  def textLDA[T] (config: LDAConfig, docExtractor: T => String)
                 (input: RDD[T]): RDD[(T, Seq[TopicScore])] = {
    val docsWithIds = input.map(t => (t, docExtractor(t))).zipWithIndex().map(_.swap)
    val ldaResults = textLDA(config, docsWithIds.map { case (key, (in, doc)) => (key, doc) })

    docsWithIds.join(ldaResults).map { case (key, ((in, doc), scores)) =>
      (in, scores)
    }
  }

  /**
    * Perform LDA on an arbitrary dataset of unindexed word bags
    */
  def wordBagLDA[T] (config: LDAConfig, wordBagExtractor: T => Map[String, Int])
                    (input: RDD[T]): RDD[(T, Seq[TopicScore])] = {
    val wordBagsWithIds = input.map(t => (t, wordBagExtractor(t))).zipWithIndex().map(_.swap)
    val ldaResults = wordBagLDA(config, wordBagsWithIds.map { case (key, (in, wordBag)) => (key, wordBag) })

    wordBagsWithIds.join(ldaResults).map { case (key, ((in, wordBag), scores)) =>
      (in, scores)
    }
  }

  /**
    * Perform LDA on an RDD of indexed documents.
    *
    * @param input An RDD of indexed documents; the Long id field should be unique for each row.
    * @return An RDD of the same documents, with a sequence of topics attached.  The third, attached, entry in each
    *         row should be read as Seq[(topic, topicScoreForDocument)], where the topic is
    *         Seq[(word, wordScoreForTopic)]
    */
  def textLDA (config: LDAConfig, input: RDD[(Long, String)]): RDD[(Long, Seq[TopicScore])] = {
    // Figure out our dictionary
    val (dictionary, documents) = textToWordCount(input)

    lda(config, dictionary, documents)
  }

  def wordBagLDA (config: LDAConfig, input: RDD[(Long, Map[String, Int])]): RDD[(Long, Seq[TopicScore])] = {
    val (dictionary, documents) = wordBagToWordCount(input)
    lda(config, dictionary, documents)
  }

  def lda (config: LDAConfig,
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
