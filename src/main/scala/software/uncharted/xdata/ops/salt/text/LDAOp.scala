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
import software.uncharted.xdata.ops.util.BasicOperations
import software.uncharted.xdata.spark.mllib.MatrixUtilities

import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, SparseVector, Vector}
import org.apache.spark.rdd.RDD

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
    * @param input
    * @return
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
    val allWords = wordLists.flatMap(_._2).reduceByKey(_ + _).collect.sortBy(-_._2)
    val dictionary = allWords.map(_._1).sorted.zipWithIndex.toMap

    // Port that dictionary back into our word maps, creating sparse vectors by map index
    val wordVectors = wordLists.map{case (id, wordList) =>
      val indices = wordList.map{case (word, count) => dictionary(word)}
      val values = wordList.map{case (word, count) => count.toDouble}
      val wordVector: Vector = new SparseVector(dictionary.size, indices, values)
      (id, wordVector)
    }
    (dictionary, wordVectors)
  }

  /**
    * Perform LDA analysis on documents in a dataframe
    *
    * @param idCol The name of a column containing a (long) id unique to each row
    * @param textCol The name of the column containing the text to analyze
    * @param numTopics The number of topics to find
    * @param topicsPerDocument The number of topics to record for each document
    * @param input The dataframe containing the data to analyze
    * @return The topics for each document
    */
  def lda(idCol: String, textCol: String, numTopics: Int, wordsPerTopic: Int, topicsPerDocument: Int,
          maxRounds: Option[Int], chkptRounds: Option[Int])(input: DataFrame): DataFrame = {
    val sqlc = input.sqlContext

    // Get the indexed documents
    val textRDD = input.select(idCol, textCol).rdd.map { row =>
      val id = row.getLong(0)
      val text = row.getString(1)
      (id, text)
    }

    // Perform our LDA analysis
    val rawResults = lda(numTopics, wordsPerTopic, topicsPerDocument, maxRounds, chkptRounds)(textRDD)
    // Mutate to dataframe form for joining with original data
    val dfResults = sqlc.createDataFrame(rawResults.map{case (index, scores) => DocumentTopics(index, scores)})

    input.join(dfResults, input(idCol) === dfResults("ldaDocumentIndex")).drop(new Column("ldaDocumentIndex"))
  }

  /**
    * Perform LDA on an RDD of indexed documents.
 *
    * @param numTopics The number of topics to find
    * @param wordsPerTopic The number of words to keep per topic
    * @param topicsPerDocument The number of topics to record for each document
    * @param input An RDD of indexed documents; the Long id field should be unique for each row.
    * @return An RDD of the same documents, with a sequence of topics attached.  The third, attached, entry in each
    *         row should be read as Seq[(topic, topicScoreForDocument)], where the topic is
    *         Seq[(word, wordScoreForTopic)]
    */
  def lda (numTopics: Int, wordsPerTopic: Int, topicsPerDocument: Int,
           maxRounds: Option[Int], chkptRounds: Option[Int])
          (input: RDD[(Long, String)]): RDD[(Long, Seq[TopicScore])] = {
    val sc = input.context

    // Figure out our dictionary
    val (dictionary, documents) = textToWordCount(input)

    val ldaEngine = new LDA()
      .setK(numTopics)
      .setOptimizer("em")
    chkptRounds.map(r => ldaEngine.setCheckpointInterval(r))
    maxRounds.map(r => ldaEngine.setMaxIterations(r))

    val model = getDistributedModel(sc, ldaEngine.run(documents))

    // Unwind the topics matrix using our dictionary (but reversed)
    val allTopics = getTopics(model, dictionary, wordsPerTopic)

    // Doc id, topics, weights
    val topicsByDocument: RDD[(Long, Array[Int], Array[Double])] = model.topTopicsPerDocument(topicsPerDocument)

    // Unwind the topics for each document
    topicsByDocument.map { case (docId, topics, weights) =>
      // Get the top n topic indices
      val topTopics = topics.zip(weights).sortBy(-_._2).take(topicsPerDocument).toSeq

      // Expand these into their topic word vectors
      (docId, topTopics.map { case (index, score) =>
        TopicScore(allTopics(index), score)
      })
    }
  }

  private def getDistributedModel (sc: SparkContext, model: LDAModel): DistributedLDAModel = {
    model match {
      case distrModel: DistributedLDAModel => distrModel
      case localModel: LocalLDAModel => {
        localModel.save(sc, tmpDir + "lda")
        DistributedLDAModel.load(sc, tmpDir + "lda")
      }
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
