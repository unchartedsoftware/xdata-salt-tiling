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
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LocalLDAModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Matrix, SparseVector, Vector}
import org.apache.spark.rdd.RDD

/**
  * An operation to run Latent Dirichlet Allocation on texts in a corpus
  */
object LDAOp {
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
    val wordLists = input.map{case (id, text) =>
        val words = text.split(notWord).map(_.toLowerCase)
        val wordCounts = MutableMap[String, Int]()
        words.foreach(word => wordCounts(word) = wordCounts.getOrElse(word, 0))
      (id, wordCounts.toList.sorted.toArray)
    }
    // Get a list of all used words, in alphabetical order.
    val dictionary = wordLists.flatMap(_._2).map(_._1).distinct.collect.sorted.zipWithIndex.toMap

    // Port that dictionary back into our word maps, creating sparse vectors by map index
    val wordVectors = wordLists.map{case (id, wordList) =>
      val indices = wordList.map{case (word, count) => dictionary(word)}
      val values = wordList.map{case (word, count) => count.toDouble}
      val wordVector: Vector = new SparseVector(wordList.size, indices, values)
      (id, wordVector)
    }
    (dictionary, wordVectors)
  }

  /**
    * Perform LDA analysis on documents in a dataframe
    * @param idCol The name of a column containing a (long) id unique to each row
    * @param textCol The name of the column containing the text to analyze
    * @param k The number of topics to find
    * @param n The number of topics to record for each document
    * @param ldaCol The name of a column into which to store the results
    * @param input The dataframe containing the data to analyze
    */
  def LDA (idCol: String, textCol: String, k: Int, n: Int, ldaCol: String)(input: DataFrame) = {
    val sc = input.sqlContext.sparkContext

    // Figure out our dictionary
    val (dictionary, documents) = textToWordCount(input.select(idCol, textCol).rdd.map{row =>
      val id = row.getLong(0)
      val text = row.getString(1)
      (id, text)
    })

    val ldaModel =
      new LDA()
        .setK(k)
        .setOptimizer("em")
        .run(documents)

    val distLDAModel =
      ldaModel match {
        case distrModel: DistributedLDAModel => distrModel
        case localModel: LocalLDAModel => {
          localModel.save(sc, tmpDir + "lda")
          DistributedLDAModel.load(sc, tmpDir + "lda")
        }
      }

    val topics: Matrix = distLDAModel.topicsMatrix
    // Doc id, topics, weights
    val results:RDD[(Long, Array[Int], Array[Double])] = distLDAModel.topTopicsPerDocument(n)

    // TODO:
    //  (1) Unwind the topics matrix using the dictionary
    //  (2) Unwind the two arrays above using the unwound topics matrix
    //  (3) Make an addColumn operation (copy from aperture tiles)
    //  (4) Use the addColumn operation to add the topic info to the dataframe, and return the result.
    null
  }
}
