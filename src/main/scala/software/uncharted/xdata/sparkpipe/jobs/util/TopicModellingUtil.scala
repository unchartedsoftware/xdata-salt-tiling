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
package software.uncharted.xdata.sparkpipe.jobs.util

import java.io.{File, PrintWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import software.uncharted.xdata.ops.topics.BTMUtil

// scalastyle:off public.methods.have.type parameter.number
/**
  * Functions for executing topic modelling jobs
  */
object TopicModellingJobUtil {

  /**
    * TODO
    * @param parts
    */
  def castResults(parts: Array[Array[Any]]) = {
    parts.map { p =>
      val date = p(0).toString
      val topic_dist = p(1).asInstanceOf[Array[(Double, Seq[String])]]
      val theta = p(2).asInstanceOf[Array[Double]]
      val phi = p(3).asInstanceOf[Array[Double]]
      val nzMap = p(4).asInstanceOf[scala.collection.mutable.HashMap[Int, Int]].toMap
      val m = p(5).asInstanceOf[Int]
      val duration = p(6).asInstanceOf[Double]
      (date, topic_dist, theta, phi, nzMap, m, duration)
    }
  }

  /**
    * Extract top 3 hashtags from each topic
    *
    * @param tp a Row of topics
    *
    */
  def find_labels(tp: Seq[String]): Seq[String] = {
    val hashtags = tp.filter(_.startsWith("#")).take(3)
    val terms = tp.filterNot(_.startsWith("#")).take(3)
    val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take (3)
    labels
  }

  /**
  * Refactored version on output_results TODO
  */
  def outputResults(
    topic_dist: Array[(Double, Seq[String])],
    nzMap: scala.collection.immutable.Map[Int, Int],
    theta: Array[Double],
    phi: Array[Double],
    date: String = "---",
    iterN: Int,
    m: Int,
    alpha: Double,
    beta: Double,
    duration: Double,
    outdir: String,
    cs: Array[Double] = Array(Double.NaN), // TODO Option
    avg_cs: Double = Double.NaN // TODO Option
  ) = {
    println(s"Writing results to directory ${outdir}")
//    val k = topic_dist.size // commented out because it was overridden by klen (which used to be 'k')
    val labeled_topic_dist = topic_dist.map{ // append 'labels' to each row
      case (theta, tpcs) => (theta, find_labels(tpcs), tpcs)
    }
    val dur = "%.4f".format(duration)
    val outfile = outdir + s"topics_${date}.txt"
    val out = new PrintWriter(new File(outfile))
    val klen = labeled_topic_dist.length
    out.println(s"# Date: $date\talpha: $alpha\tbeta: $beta\titerN: $iterN\tM: $m\tK: $klen")
    out.println(s"# Running time:\t$dur min.")
    out.println(s"Average Coherence Score: $avg_cs")
    out.println(s"Coherence scores: " + cs.mkString(", "))
    out.println("#" + "-" * 80)
    out.println("#Z\tCount\tp(z)\t\t\tTop terms descending")
    out.println("#" + "-" * 80)
    labeled_topic_dist.zipWithIndex.map {
      case (td, i) => i + "\t" + nzMap(i) + "\t" + td._1 + "\t" + td._2.mkString(", ") + "\t->\t" + td._3.take(20).mkString(", ")
    } foreach {
      out.println
    }
    out.close
  }

  /**
    * Topic Modelling
    * TODO diff between this and loadTweets?
    * Load a sample of tweets I previously preprocessed for experiments. Schema => (YMD, id, text)
    */
  def loadCleanTweets(
    sc : SparkContext,
    path: String,
    dates: Array[String],
    caIdx: Int = 0,
    idIdx: Int = 1,
    textIdx: Int = 2
  ) = {
    sc.textFile(path) // TODO inject sparkcontext
      .map(_.split("\t"))
      .filter(x => x.length > textIdx)
      .filter(x => dates contains x(caIdx))
  }

  /**
    * Reads an RDD of tweets from the given source (tab seperated) data
    *
    * @param path path to data in hdfs
    * @param dates array of dates to run this job on
    * @param caIdx created_at index
    * @param idIdx twitter_id index
    * @param textIdx text index
    * @return an rdd of the source data
    */
  def loadTweets(
    sc : SparkContext,
    path: String,
    dates: Array[String],
    caIdx: Int = 0,
    idIdx: Int = 1,
    textIdx: Int = 2
  ) : RDD[Array[String]] = {
    sc.textFile(path)
      .map(_.split("\t"))
      .filter(x => x.length > textIdx)
      .map(x => Array(x(caIdx), x(idIdx), x(textIdx) ))
      .map{ case Array(d, i, t) => Array(BTMUtil.ca2ymd(d), i, t) }
      .filter{ case Array( d, i, t) => dates contains d }
  }

  /**
    * Reads an RDD of dates from the given source (tab seperated) data
    *
    * @param path path to data in hdfs
    * @param dates array of dates to run this job on
    * @param caIdx created_at index
    * @param idIdx twitter_id index
    * @param textIdx text index
    * @return an rdd of the source data
    */
    def loadDates(
      sc : SparkContext,
      path: String,
      dates: List[String],
      caIdx: Int,
      idIdx: Int,
      textIdx: Int
    ) = {
      sc.textFile(path)
        .map(_.split("\t"))
        .filter(x => x.length > textIdx)
        .filter(x => dates contains x(caIdx))
    }
}
