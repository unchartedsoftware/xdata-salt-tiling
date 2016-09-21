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

package software.uncharted.xdata.ops.topics

import scala.math._
import scala.io.Source
import org.apache.spark.rdd.RDD
import java.io._
import grizzled.slf4j.Logging

// :load cleanText_BTM.scala
// :load BTMUtil.scala

/*
spark-shell --master yarn-client  --executor-cores 2  --num-executors 4  --executor-memory 5G
 */
object Coherence extends Serializable with Logging {
//  def getTokens(arr: Array[String]) = {
//    val tokens = arr.map(text => TextUtil.cleanText(text)).map(_.split("\\s+"))
//    sc.parallelize(tokens)
//  }

  def getTokensRdd(rdd: RDD[String]) = {
    val tokens = rdd.map(text => TextUtil.cleanText(text)).map(_.split("\\s+"))
    tokens
  }

  def getVocabFromTopicDist(topic_dist: Array[(Double, Seq[String])] , topT: Int) = {
    val vocabulary = topic_dist.map{ case (theta, tpcs) => tpcs }.map(x => x.take(topT)).flatMap(x => x).toSet
    vocabulary
  }

  def getTopicsFromTopicDistFile(topic_dist_file: String, termIdx: Int): Array[Array[String]] = {
    val topics = Source.fromFile(topic_dist_file).getLines.filter(x => !(x.startsWith("#"))).map(_.split("\t")).map(x => x(termIdx)).map(x => x.split(",\\s?")).toArray
    topics
  }

  def topTwordsFromTopics(topics: Array[Array[String]], topT: Int) = {
    val ttopics = topics.map(x => x.take(topT))
    ttopics.flatMap(x => x).toSet
  }

  // Nathan's improvement on the function below - doesn't involve Java boxing - unboxing
  def toDouble (number: Any): Double = number match {
    case n: Double => n
    case n: Int => n.toDouble
  }

  // Cast a number which is AnyType to Double
  def castDouble(number: Any): Double = number match {
    case n: Double => n.doubleValue()
    case n: Int    => n.toDouble
    case x         => throw new IllegalArgumentException(s"$x is not a number.")
  }

  // document frequency of a word
  def unigramDocFreq(tokens: RDD[Array[String]], vocabulary: Set[String]) = {
    val wc = tokens.map(x => x.filter(vocabulary contains _)).filter(x => x.size > 0).map(x => x.toSet.toSeq).flatMap(x => x).map(x => (x, 1)).reduceByKey(_ + _)
    val df = wc.collect.toMap
    df
  }

  // document frequency of a bigram
  def bitermDocFreq(tokens: RDD[Array[String]], vocabulary: Set[String]) = {
    // def getWordBiterms(d: Array[String]): Iterator[(String, String)] = {
    //     d.toSeq.combinations(2).map {
    //         case Seq(w1, w2) => if (w1 < w2) (w1, w2) else (w2, w1)
    //     }
    // }
    def getWordBiterms(d: Array[String]): Iterator[(String, String)] = {
      val bt = d.combinations(2).map { case Array(w1, w2) => if (w1 < w2) (w1, w2) else (w2, w1) }
      bt
    }
    val biterms = tokens.map(x => x.filter(vocabulary contains _)).filter(x => x.size > 1).map(x => getWordBiterms(x))
    info(s"\n biterms.size = ${biterms.count}\n")
    val bi_wc = biterms.flatMap(x => x).map(x => (x, 1)).reduceByKey(_ + _).filter(x => x._2 > 1)
    info(s"bi_wc ... bi_df ...")
    val bi_df = bi_wc.collect.toMap
    info(s"bi_df.size = ${bi_df.size}")
    bi_df
  }

  def topic_coherence(topic: Array[String], unigram_df: Map[String,Int], biterm_df: Map[(String, String),Int], epsilon: Double = 1 ) = {
    val cowords = topic.toSeq.combinations(2).toArray
    val score = cowords.map { case Seq(w1, w2) =>
      val numerator = biterm_df.getOrElse(if (w1 < w2) (w1, w2) else (w2, w1), 0) + epsilon
      val denominator = unigram_df.getOrElse(w1, numerator)
      val c = log(castDouble(numerator) / castDouble(denominator))
      c
    }.sum
    score
  }


  // def coherence_scores(td: Array[(Double, Seq[String])], unigram_df: Map[String,Int], biterm_df: Map[(String, String),Int], threshold: Double = -0.01 ) = {
  //     // Calculate Average Coherence Score; ignore topics with count < 100
  //     val cs = td.filter(x => x._1 > threshold ).map { case (theta, tpcs )  =>
  //         val coherence = topic_coherence(tpcs, unigram_df, biterm_df)
  //         coherence
  //     }
  //     cs
  // }

  // def avg_coherence(td: Array[(Double, Seq[String])], unigram_df: Map[String,Int], biterm_df: Map[(String, String),Int], threshold: Double = 0.01 ) = {
  //     // Calculate Average Coherence Score; ignore topics with probability < threshold (default = 1%)
  //     // val cs = td.filter(x => x._1 > threshold ).map { case (theta, tpcs )  =>
  //     //     val coherence_score = topic_coherence(tpcs, unigram_df, biterm_df)
  //     //     coherence_score
  //     // }
  //     val cs = coherence_scores(td, unigram_df, biterm_df, threshold)
  //     val avg_cs = cs.sum / cs.size
  //     avg_cs
  // }


  /**
   * @param rdd          RDD containing JUST the cleaned text
   * @param topic_terms  the top words (just the words, not theta) returned by running topic modeling (output as topic_dist)
   * @param topT         the top T words which to consider in computing coherence scores
   * @return       (Array of coherence scores (one per topic), average coherence score for input data)
   */
  def computeCoherence(rdd: RDD[String], topic_terms: Array[Array[String]], topT: Int) = {
    val topT_topics = topic_terms.map(x => x.take(topT)).toSeq
    val vocabulary = topTwordsFromTopics(topic_terms, topT)
    // convert documents into token arrays
    val tokens = getTokensRdd(rdd).map(x => x.filter(vocabulary contains _)).cache
    // compute D(v) and D(V, v'), return a Map of each
    info("calculating unigram doc freq...")
    val unigram_df = unigramDocFreq(tokens, vocabulary)
    info("calculating biterm doc freq...")
    val biterm_df = bitermDocFreq(tokens, vocabulary)
    info("calculating topic coherence...")
    val cs = topT_topics.map{tpcs  =>
      val coh = topic_coherence(tpcs, unigram_df, biterm_df)
      coh
    }
    val avg_cs = cs.sum / cs.size
    (cs, avg_cs)
  }


//  /**
//   * @param rdd          RDD containing JUST the cleaned text
//   * @param topic_terms  the top words (just the words, not theta) returned by running topic modeling (output as topic_dist)
//   * topT         the top T words which to consider in computing coherence scores
//   * RETURN       (Array of coherence scores (one per topic), average coherence score for input data)
//   */
//  def computeCoherenceLocal(texts: Array[String], topic_terms: Array[Array[String]], topT: Int) = {
//    val topT_topics = topic_terms.map(x => x.take(topT)).toSeq
//    val vocabulary = topTwordsFromTopics(topic_terms, topT)
//    // convert documents into token arrays
//    val tokens = texts.map(text => TextUtil.cleanText(text)).map(_.split("\\s+")).map(x => x.filter(vocabulary contains _))
//
//    // compute D(v) and D(V, v'), return a Map of each
//    info("calculating unigram doc freq...")
//    val unigram_df = unigramDocFreq(tokens, vocabulary)
//    info("calculating biterm doc freq...")
//    val biterm_df = bitermDocFreq(tokens, vocabulary)
//    info("calculating topic coherence...")
//    val cs = topT_topics.map{tpcs  =>
//      val coh = topic_coherence(tpcs, unigram_df, biterm_df)
//      coh
//    }
//    val avg_cs = cs.sum / cs.size
//    (cs, avg_cs)
//  }
}
