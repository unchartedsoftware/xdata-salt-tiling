/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.xdata.ops.topics.twitter.util

import org.apache.spark.sql.types._

import software.uncharted.xdata.spark.SparkFunSpec
import software.uncharted.sparkpipe.ops.xdata.util.DataFrameOperations.toDataFrame

class TFIDFTest extends SparkFunSpec {
  private val testSequence = Seq("2017-0101,cat,1.0", "2017-0105,books,1.5", "2015-0101,plants,1.6", "2016-0101,trees,3.9")

  describe("#test various TFIDF functions") {

    it("should test loadTFIDF") {
      val data = sc.parallelize(testSequence)
      val schema = StructType(Seq(StructField("a", StringType), StructField("b", StringType), StructField("c", DoubleType)))
      val converted = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      val sampleTFIDF = TFIDF.loadTFIDF(converted)
      val expected = Array(("2017-0101","cat", 1.0),("2017-0105", "books", 1.5),("2015-0101", "plants", 1.6), ("2016-0101", "trees", 3.9))

      assertResult(expected)(sampleTFIDF)
    }

    it ("should should filter a TFIDF based on a date range") {
      val data = sc.parallelize(testSequence)
      val schema = StructType(Seq(StructField("a", StringType), StructField("b", StringType), StructField("c", DoubleType)))
      val df = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      val sampleTFIDF = TFIDF.loadTFIDF(df)
      val dates = Array("2017-0101", "2017-0105")
      val result = TFIDF.filterDateRange(sampleTFIDF, dates)
      val expected = Array(("2017-0101","cat",1.0), ("2017-0105","books",1.5))

      assertResult(expected)(result)
    }

    it ("should filter a TFIDF based on a given word dictionary") {
      val data = sc.parallelize(testSequence)
      val schema = StructType(Seq(StructField("a", StringType), StructField("b", StringType), StructField("c", DoubleType)))
      val df = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      val sampleTFIDF = TFIDF.loadTFIDF(df)
      val word_dict = Map("cat"->1, "trees"->2)
      val result = TFIDF.filterWordDict(sampleTFIDF, word_dict)
      val expected = Array(("2017-0101","cat",1.0), ("2016-0101","trees",3.9))

      assertResult(expected)(result)
    }

    it ("should convert a tfidf array to a map") {
      val data = sc.parallelize(testSequence)
      val schema = StructType(Seq(StructField("a", StringType), StructField("b", StringType), StructField("c", DoubleType)))
      val df = toDataFrame(sparkSession, Map[String, String](), schema)(data)
      val sampleTFIDF = TFIDF.loadTFIDF(df)
      val word_dict = Map("cat"->1, "trees"->2, "books"->5, "plants"->3)
      val result = TFIDF.tfidfToMap(sampleTFIDF, word_dict)
      val expected = Map(1->1.0, 5->1.5, 3->1.6, 2->3.9)

      assertResult(expected)(result)
    }
  }

}
