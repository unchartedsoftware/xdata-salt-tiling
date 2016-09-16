
/*
Difference between BDP_parallel and BDP:
    - DatePartitioner class
    -convert texts to key value pairs (date, text) or (date, (id, text))
  Pretty much everything else is the same and runs the sam as BDP, just on separate partiions.
*/


package com.uncharted.btm

import java.io.Serializable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD




class DatePartitioner (dates: Array[String]) extends Partitioner {
  val dateMap = dates.zipWithIndex.toMap

  override def numPartitions: Int = {
    val num_parts = dates.size
    num_parts
  }
  override def getPartition(key: Any): Int =  {
    val part = dateMap.get(key.toString).get
    part
  }
}




object BDPParallel  extends Serializable {

  // **************************************************************************************************************
  //          Main Topic Modeling Function
  // **************************************************************************************************************
def partitionBDP(iterator: Iterator[(String, (String, String))],  stpbroad: Broadcast[scala.collection.immutable.Set[String]], iterN: Int, k:Int, alpha: Double, eta: Double, weighted: Boolean = false, tfidf_path: String = "") = {
    val datetexts = iterator.toSeq.map(x => (x._1, x._2._2))
    val date = datetexts.map(x => x._1).toSet.toArray.head.asInstanceOf[String]
    // val texts = datetexts.map(x => x._2)                        // all tweets
    val texts = datetexts.map(x => x._2).distinct.toArray               // no retweets

    val stopwords = stpbroad.value
    val minCount = 0
    val (word_dict, words) = WordDict.createWordDictLocal(texts, stopwords, minCount)
    val m = words.size

    val bdp = new BDP(k)

    val biterms0 = texts.map(text => BTMUtil.extractBitermsFromTextRandomK(text, word_dict, stopwords.toSet, k)).flatMap(x => x)
    val biterms = biterms0.toArray

    if (weighted) bdp.initTfidf(tfidf_path, date, word_dict)
    val (topic_dist, theta, phi, nzMap, duration) = bdp.fit(biterms, words, iterN, k, alpha, eta, weighted)
    val result = List(Array(date, topic_dist, theta, phi, nzMap, m, duration)).iterator

    result
  }



  // **************************************************************************************************************
  //          Utility Functions
  // **************************************************************************************************************
  def keyvalueRDD(rdd: RDD[Array[String]]) = {
    val kvrdd = rdd.map(x => (x(0), (x(1), x(2)))  )
    kvrdd
  }




}





