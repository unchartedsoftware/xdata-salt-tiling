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

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
//import org.joda.time.Days
//import org.joda.time.DateTime
//import org.joda.time.Period

// scalastyle:off public.methods.have.type parameter.number
/**
  * Functions for executing topic modelling jobs
  */
object TopicModellingUtil extends Logging {

  /**
    * Create a sequence of dates between the given endpoints with one day intervals
    *
    * @param from The date to start the range from
    * @param to The date to end the range on
    * @return A seqence of dates (of the format yyyy-MM-dd) between the given endpoints with one day intervals
    */
//  def dateRange(from: Date, to: Date): Seq[String] = {
//    val s = Iterator.iterate(new DateTime(from))(_.plus(new Period().withDays(1))).takeWhile(!_.isAfter(new DateTime(to))).toSeq
//    s.map(datetime => datetime.toString.slice(0, 10))
//  }

  /**
  * Create a sequence of dates between the given endpoints with one day intervals
  *
  * @param from The date to start the range from (yyyy-MM-dd)
  * @param to The date to end the range on (yyyy-MM-dd)
  * @return A seqence of dates (of the format yyyy-MM-dd) between the given endpoints with one day intervals
  */
//  def dateRange(from: String, to: String): Seq[String] = {
//    val format = new SimpleDateFormat("yyyy-MM-dd") // if we add the ymd date cal first we dont need the formatter TODO can use other signature of this function
//    dateRange(format.parse(from), format.parse(to))
//  }

  /**
    * Cast the results of BDP operation and cast them into their appropriate types
    *
    * @param parts An array of results, one for each partition/date the operation was run on
    */
  def castResults(parts: Array[Array[Any]]) : Array[(String, Array[(Double, Seq[String])], Array[Double], Array[Double], Map[Int,Int], Int, Double)] = {
    parts.map { p =>
      val date = p(0).toString
      val topic_dist = p(1).asInstanceOf[Array[(Double, Seq[String])]]
      val theta = p(2).asInstanceOf[Array[Double]]
      val phi = p(3).asInstanceOf[Array[Double]]
      val nzMap = p(4).asInstanceOf[collection.immutable.Map[Int,Int]]
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
  def findLabels(tp: Seq[String]): Seq[String] = {
    val hashtags = tp.filter(_.startsWith("#")).take(3)
    val terms = tp.filterNot(_.startsWith("#")).take(3)
    val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take (3)
    labels
  }

  def writeResultsToFile(
    cparts : Array[(String, Array[(Double, Seq[String])], Array[Double], Array[Double], Map[Int,Int], Int, Double)],
    data: DataFrame,
    textCol : String,
    alpha: Double,
    beta: Double,
    outdir: String,
    iterN : Int,
    computeCoherence : Boolean,
    numTopTopics : Int
  ) = {
    cparts.foreach { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
      var cs : Option[Array[Double]] = None
      var avg_cs : Option[Double] = None
      if (computeCoherence) { // n.b. compute and output coherence scores
        val topic_terms = topic_dist.map(x => x._2.toArray)
        val textrdd = data.select(textCol).rdd.map(r => r(r.fieldIndex(textCol)).toString)
        val (_cs, _avg_cs) = Coherence.computeCoherence(textrdd, topic_terms, numTopTopics)
        cs = Some(_cs.toArray)
        avg_cs = Some(_avg_cs)
      }

      println(s"Writing results to directory $outdir") // TODO make sure directory exists. Error otherwise
      val labeled_topic_dist = topic_dist.map{ // append 'labels' to each row
        case (theta, tpcs) => (theta, findLabels(tpcs), tpcs)
      }
      val now = Calendar.getInstance().getTime
      val minuteFormat = new SimpleDateFormat("mm")
      val dur = "%.4f".format(duration)
      val outfile = outdir + s"topics_$date.txt"
      val out = new PrintWriter(new File(outfile))
      val klen = labeled_topic_dist.length
      out.println(s"# Date: $date\talpha: $alpha\tbeta: $beta\titerN: $iterN\tM: $m\tK: $klen")
      out.println(s"# Running time:\t$dur min.")
      out.println(s"Average Coherence Score: " + avg_cs.getOrElse().toString)
      out.println(s"Coherence scores: " + cs.getOrElse(Array()).mkString(", "))
      out.println("#" + "-" * 80)
      out.println("#Z\tCount\tp(z)\t\t\tTop terms descending")
      out.println("#" + "-" * 80)
      labeled_topic_dist.zipWithIndex.map {
        case (td, i) => i + "\t" + nzMap(i) + "\t" + td._1 + "\t" + td._2.mkString(", ") + "\t->\t" + td._3.take(20).mkString(", ")
      } foreach {
        out.println
      }
      out.close()
    }
  }
}
