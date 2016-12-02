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

package software.uncharted.xdata.ops.topics.twitter

import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import software.uncharted.sparkpipe.Pipe
import software.uncharted.xdata.ops.salt.RangeDescription
import software.uncharted.xdata.ops.topics.twitter.util.WordDict
import software.uncharted.xdata.spark.SparkFunSpec

import scala.collection.mutable

class TwitterTest extends SparkFunSpec {
  describe("getDocumentTopic()") {
    val oldDir = System.getProperty("user.dir")
    try {
      val path = classOf[TwitterTest].getResource("/topic-modelling/topic-modelling.conf").toURI.getPath
      // Run the test from path/to/xdata-pipeline-ops/
      val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
      System.setProperty("user.dir", newDir)

      val stopWords = WordDict.loadStopwords(Array(
        "src/main/resources/stopwords/stopwords_all_en.v2.txt",
        "src/main/resources/stopwords/stopwords_ar.txt",
        "src/main/resources/stopwords/stopwords_html_tags.txt"
      ).toList)

      it("should label tweets with topics") {
        //Basic test to make sure overall process runs.
        val rddData = sc.parallelize(Seq(
          Tweet("1", "Sat Oct 01 00:00:10 +0000 2016", "The TPP needs to be killed!"),
          Tweet("2", "Sat Oct 01 02:00:10 +0000 2016", "Death to all!!"),
          Tweet("3", "Sat Oct 01 04:00:10 +0000 2016", "ISIS is death"),
          Tweet("4", "Sun Oct 02 00:00:10 +0000 2016", "i wish i had been independent and wise because this is silly"),
          Tweet("5", "Sun Oct 02 00:00:10 +0000 2016", "this is the final test i did it. Now, more than ever, we need Trump.")
        ))
        val stopwords_bcst = sqlc.sparkContext.broadcast(stopWords)

        val dfData = sqlc.createDataFrame(rddData)

        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val minTime = formatter.parse("2016-09-30").getTime
        val maxTime = formatter.parse("2016-10-03").getTime
        val timeRange = RangeDescription.fromStep(minTime, maxTime, 24 * 60 * 60 * 1000).asInstanceOf[RangeDescription[Long]]

        val result = getDocumentTopic(
          "date",
          "id",
          "text",
          "topic",
          Some(1 / Math.E),
          Some(0.01),
          timeRange,
          Some(10),
          Some(2),
          Some(10),
          stopwords_bcst,
          None)(dfData)

        //Every row should be returned.
        assert(result.count() === 5)

        //The output field should exist.
        assert(result.schema.fieldIndex("topic") > 0)
      }

      it("should run a complete corpus and return tweets labelled with topics") {
        //Run a bigger test and make sure the returned topics are generally accurate.
        //You will never be able to get 100% identical result, but the general content of the returned topics
        //will be consistent between runs.
        val reader_corpus = sqlc.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "\t")
        val corpus = Pipe(reader_corpus.load("src/test/resources/topic-modelling/isil_keywords.20160901-000000.txt")).run().filter("C7 = 'en'")
        val stopwords_bcst = sqlc.sparkContext.broadcast(stopWords)

        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val minTime = formatter.parse("2016-08-30").getTime
        val maxTime = formatter.parse("2016-09-03").getTime
        val timeRange = RangeDescription.fromStep(minTime, maxTime, 24 * 60 * 60 * 1000).asInstanceOf[RangeDescription[Long]]

        val result = getDocumentTopic(
          "date",
          "id",
          "text",
          "topic",
          Some(1 / Math.E),
          Some(0.01),
          timeRange,
          Some(10),
          Some(5),
          Some(3),
          stopwords_bcst,
          None)(corpus)

        //Every row should be returned.
        assert(result.count() === corpus.count())

        //The output field should exist.
        assert(result.schema.fieldIndex("topic") > 0)

        val resultC = result.select("text", "topic").collect()

        //Group by topic. isis, joke, pewdiepie, youtube & twitter should make up the top 5.
        val expectedTopics = Array("isis", "joke", "twitter", "youtube", "pewdiepie")
        val topicCounts =
          resultC.flatMap(r => r(1).asInstanceOf[mutable.WrappedArray[Any]]
            .map(r => r.asInstanceOf[Row])
            .map(r => (r(0).asInstanceOf[String], r(1).asInstanceOf[Double])))
            .groupBy(t => t._1).map(g => (g._1, g._2.length))
        val unexpectedCount = topicCounts.toArray.sortBy(tc => -tc._2).take(5)
          .filter(x => !expectedTopics.contains(x._1)).length
        assert(unexpectedCount === 0)
      }
    } finally {
      System.setProperty("user.dir", oldDir)
    }
  }
}

case class Tweet (id: String, date: String, text: String)
