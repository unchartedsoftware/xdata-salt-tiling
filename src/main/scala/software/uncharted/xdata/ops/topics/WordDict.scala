
package com.uncharted.btm


import scala.io.Source
import org.apache.spark.rdd.RDD


object WordDict extends Serializable {
  def createDictFromArray(words: Array[String]) = {
    Array.tabulate(words.length)(i => (words(i) -> i) ).toMap
  }

  def wordcount2WordDict(wordcount: Array[(String, Int)], stopwords: Set[String], min_count: Int = 1) = {
    val filtered = wordcount.filter{case (word, count) => word.length > 2 }              // exclude words with 1-2 characters
                            .filter{case (word, count) => count >  min_count }            // exclude words with counts under min_count
                            .filterNot{case (word, count) => "^[#0-9]+$".r.findFirstIn(word).isDefined }   // exclude words made up of only digits
                            .filterNot{case (word, count) => "^[\\s!\"#$%&\\\\\\'()*+,-./:;<=>?@\\[\\]^_`{|}~]+$".r.findFirstIn(word).isDefined  }   // exclude words made up of only punctuation
                            .filterNot{case (word, count) => stopwords contains word }       // exclude stopwords
    val sorted = filtered.sortWith{ (a, b) => a._2 > b._2 }
    val words = sorted.map{case (word, count) => word}
    val word_dict = createDictFromArray(words)
    (word_dict, words)
  }

  def computeWordCount(rdd: RDD[String], minCount: Int = 5) = {
    // extract text only, clean text of punctuation & lowercase
    val cleanRdd = rdd.map(x => TextUtil.cleanText(x) ).cache
    // find out which words we REALLY need in the word_dict (to make it smaller)
    val wc = cleanRdd.map(x => x.split("\\s+")).flatMap(x => x).map(x => (x, 1)).reduceByKey(_ + _).collect.filter(x => x._2 > minCount)
    wc
  }

  def computeWordCountLocal(arr: Array[String], minCount: Int = 5) = {
    // extract text only, clean text of punctuation & lowercase
    val cleaned = arr.map(x => TextUtil.cleanText(x) )
    // find out which words we REALLY need in the word_dict (to make it smaller)
    val tokens = cleaned.map(x => x.split("\\s+")).flatMap(x => x)
    val wc = tokens.groupBy(_.toString).map(x => (x._1, x._2.size)).filter(x => x._2 > minCount).toArray
    wc
  }

  def loadStopwords(swfiles: List[String]) = {
    val stopwords = swfiles.map ( swpath => Source.fromFile(swpath).getLines.toArray).flatMap( w => w).toSet
    stopwords
  }

  def createWordDict(rdd: RDD[String], stopwords: Set[String], minCount: Int) = {
    val wc = computeWordCount(rdd, minCount)
    println(s"wc contains ${wc.size} words")
    println("computing word dictionary...")
    val (word_dict, words) = wordcount2WordDict(wc, stopwords, minCount)
    (word_dict, words)
  }

  def createWordDictLocal(arr: Array[String], stopwords: Set[String], minCount: Int) = {
    val wc = computeWordCountLocal(arr, minCount)
    println(s"wc contains ${wc.size} words")
    println("computing word dictionary...")
    val (word_dict, words) = wordcount2WordDict(wc, stopwords, minCount)
    (word_dict, words)
  }

}


