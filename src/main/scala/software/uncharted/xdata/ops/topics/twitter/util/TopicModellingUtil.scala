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
package software.uncharted.xdata.ops.topics.twitter.util

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar,Date}
import java.nio.file.{Paths, Files}
import grizzled.slf4j.{Logging, Logger}
import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, Days, Period}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, IntegerType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType}

import org.apache.spark.sql.{SQLContext}

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
 def dateRange(from: Date, to: Date): Array[String] = {
   val s = Iterator.iterate(new DateTime(from))(_.plus(new Period().withDays(1))).takeWhile(!_.isAfter(new DateTime(to))).toArray
   s.map(datetime => datetime.toString.slice(0, 10))
 }

  /**
  * Create a sequence of dates between the given endpoints with one day intervals
  *
  * @param from The date to start the range from (yyyy-MM-dd)
  * @param to The date to end the range on (yyyy-MM-dd)
  * @return A seqence of dates (of the format yyyy-MM-dd) between the given endpoints with one day intervals
  */
 def dateRange(from: String, to: String): Array[String] = {
   val format = new SimpleDateFormat("yyyy-MM-dd")
   dateRange(format.parse(from), format.parse(to))
 }

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
    val labels = if (hashtags.size >= 3) hashtags else hashtags ++ terms take 3
    labels
  }

  // scalastyle:off method.length
  /**
    * Write the results of topic modelling to file. Writes one result file per day in the date range given as input
    *
    * @param cparts
    * @param data
    * @param textCol
    * @param alpha
    * @param beta
    * @param outdir
    * @param iterN
    * @param computeCoherence
    * @param numTopTopics
    */
  def writeTopicsToDF(
    alpha: Double,
    beta: Double,
    coherenceMap: Option[Map[String, (Seq[Double], Double)]],
    computeCoherence : Boolean,
    cparts : Array[(String, Array[(Double, Seq[String])], Array[Double], Array[Double], Map[Int,Int], Int, Double)],
    data: DataFrame,
    iterN : Int,
    numTopTopics : Int,
    outdir: String,
    sqlContext: SQLContext,
    textCol : String
  ) : DataFrame = {

    var result : scala.collection.mutable.Seq[Row] = scala.collection.mutable.Seq()
    cparts.map { cp =>
      val (date, topic_dist, theta, phi, nzMap, m, duration) = cp
      topic_dist.zipWithIndex.map {
        case (t, i) => result = result ++ Seq(Row(
          date,
          i,
          nzMap(i),
          t._1, // theta
          findLabels(t._2), // topic labels
          t._2.slice(0,20), // topics
          "%.4f".format(duration), // job duration
          alpha,
          beta,
          iterN,
          m,
          topic_dist.length, // k
          if (!coherenceMap.isEmpty) {val score = coherenceMap.get(date); score._1(i)} else None, // coherence score TODO index at i
          if (!coherenceMap.isEmpty) {val avg = coherenceMap.get(date); avg._2} else None // average coherence score
        ))
      }
    }

    /**
      * Output schema for results. Results are segmented by date
      */
    val schema = StructType(
      StructField("date", StringType, false) ::
      StructField("Z", IntegerType, false) ::
      StructField("nzmap(i) TODO", IntegerType, false) ::
      StructField("theta", DoubleType, false) ::
      StructField("topic_labels", ArrayType(StringType, false), false) ::
      StructField("topics", ArrayType(StringType, false), false) ::
      StructField("job_duration_minutes", StringType, false) ::
      StructField("alpha", DoubleType, false) ::
      StructField("beta", DoubleType, false) ::
      StructField("iterN", IntegerType, false) ::
      StructField("m", IntegerType, false) ::
      StructField("k", IntegerType, false) ::
      StructField("coherence_scores", DoubleType, true) ::
      StructField("average_coherence_score", DoubleType, true) ::
      Nil
    )

    val rdd = sqlContext.sparkContext.parallelize(result.toSeq)

    sqlContext.createDataFrame(rdd, schema)
  }
}
