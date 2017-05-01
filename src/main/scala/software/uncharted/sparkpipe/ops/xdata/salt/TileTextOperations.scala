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
package software.uncharted.sparkpipe.ops.xdata.salt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.generation.request.TileLevelRequest
import software.uncharted.salt.core.projection.numeric.NumericProjection
import software.uncharted.salt.xdata.analytic.WordCounter
import software.uncharted.sparkpipe.ops.xdata.text.transformations.{getDictionaries, getDictionary}
import software.uncharted.xdata.tiling.config.TFIDFConfig

import scala.reflect.ClassTag

object TileTextOperations extends ZXYOp {
  type TileData[T, X] = SeriesData[(Int, Int, Int), (Int, Int), T, X]

  /**
    * Take a dataframe with a column containing text documents, and tile the documents into term-frequency collections.
    *
    * @param xCol The column with the x coordinate of each record
    * @param yCol The column with the y coordinate of each record
    * @param textCol The column containing the document in each record
    * @param projection The projection dermining how x and y coordinates are interpretted
    * @param zoomLevels The tile levels to generate
    * @param input The input data
    * @return A set of tiles containing, by tile, the words appearing in each tile, and the number of times each word
    *         appears
    */
  def termFrequencyOp(xCol: String,
                      yCol: String,
                      textCol: String,
                      projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
                      zoomLevels: Seq[Int])
                     (input: DataFrame):
  RDD[SeriesData[(Int, Int, Int), (Int, Int), Map[String, Int], Nothing]] = {
    // Pull out term and document frequencies for each tile
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    val binAggregator = new WordCounter

    super.apply(projection, 1, xCol, yCol, textCol, binAggregator, None)(request)(input)
  }

  /**
    * Take a dataframe containing documents, and transform it to a tile set of word bags - transforming each document
    * into a word bag, then combining them.
    *
    * @param xColumn The column of the input DataFrame containing the x coordinate of each document
    * @param yColumn The column of the input DataFrame containing the y coordinate of each document
    * @param documentColumn  The column of the input DataFrame containing the documents to be analyzed
    * @param projection The projection from (x, y) to tile space
    * @param zoomLevels The zoom levels to tile
    * @param input The input data
    * @return
    */
  def tileWordBags(xColumn: String,
                   yColumn: String,
                   documentColumn: String,
                   projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
                   zoomLevels: Seq[Int])
                  (input: DataFrame):
  RDD[TileData[Map[String, Int], Nothing]] = {
    // Pull out term and document frequencies for each tile
    val request = new TileLevelRequest(zoomLevels, (tc: (Int, Int, Int)) => tc._1)
    val binAggregator = new WordCounter

    super.apply(projection, 1, xColumn, yColumn, documentColumn, binAggregator, None)(request)(input)
  }


    /**
    * Perform TFIDF on the output of termFrequency, on a tile by tile basis
    *
    * This version operates level by level; it is easier to understand, but slower than version 2 below.
    *
    * @param config Configuration specifying how to perform TF*IDF analytic
    * @param input
    * @return
    */
  def doTFIDFByTileSlow[X](config: TFIDFConfig)(input: RDD[TileData[Map[String, Int], X]])
  : RDD[TileData[List[(String, Double)], X]] = {
    // Cache input - we're going to be going through it a lot
    input.cache()
    val levels = input.map(_.coords._1).distinct.collect().sorted
    val results = for (level <- levels) yield {
      val lvlData = input.filter(_.coords._1 == level)
      val lvlDocs = lvlData.flatMap(_.bins.seq).filter(!_.isEmpty).count
      val lvlDict = getDictionary[Map[String, Int]](config.dictionaryConfig, map => map)(lvlData.flatMap(_.bins.seq))
      val lvlIdfs = lvlDict.map { case (term, documentsWithTerm) =>
        (term, config.idf.inverseDocumentFrequency(lvlDocs, documentsWithTerm))
      }.toMap
      tileTransformationOp((binData: Map[String, Int], tile: (Int, Int, Int), bin: (Int, Int)) => {
        if (binData.isEmpty) {
          List[(String, Double)]()
        } else {
          val maxRawFrequency = binData.map(_._2).max
          val terms = binData.size
          binData.map { case (term, rawFrequency) =>
            val tf = config.tf.termFrequency(rawFrequency, terms, maxRawFrequency)
            val idf = lvlIdfs(term)
            (term, tf * idf)
          }.toList.sortBy(_._2).take(config.wordsToKeep)
        }
      })(lvlData)
    }
    results.reduce(_ union _)
  }

  /**
    * Perform TFIDF on the output of termFrequency, on a tile by tile basis
    *
    * This version acts on all levels at once, so is faster than the level by level version, but more complex.
    *
    * @param config Configuration specifying how to perform TF*IDF analytic
    * @param input
    * @return
    */
  def doTFIDFByTileFast[X](config: TFIDFConfig)(input: RDD[TileData[Map[String, Int], X]])
  : RDD[TileData[List[(String, Double)], X]] = {
    input.cache
    val docsWithLevel = input.flatMap(datum => datum.bins.seq.map(bin => (datum.coords._1, bin)))
    val dictionary = getDictionaries[(Int, Map[String, Int])](config.dictionaryConfig, datum => datum)(docsWithLevel)
    val docCountByLevel = docsWithLevel.map { case (level, doc) =>
      if (doc.isEmpty) {
        (level, 0)
      } else {
        (level, 1)
      }
    }.reduceByKey(_ + _).collect.toMap
    val idfs = dictionary.map { case (term, termDocsByLevel) =>
      (
        term,
        termDocsByLevel.map { case (level, documentsWithTerm) =>
          (level, config.idf.inverseDocumentFrequency(docCountByLevel(level), documentsWithTerm))
        }
        )
    }.toMap

    tileTransformationOp((binData: Map[String, Int], tile: (Int, Int, Int), bin: (Int, Int)) => {
      if (binData.isEmpty) {
        List[(String, Double)]()
      } else {
        val level = tile._1
        // Filter out terms that aren't in our dictionary
        val knownWords = binData.filter { case (term, termFrequency) =>
            idfs.contains(term)
        }

        val maxRawFrequency = knownWords.map(_._2).max
        val terms = knownWords.size

        binData.map { case (term, rawFrequency) =>
          val tf = config.tf.termFrequency(rawFrequency, terms, maxRawFrequency)
          val idf = idfs(term)(level)
          (term, tf * idf)
        }.toList.sortBy(_._2).take(config.wordsToKeep)
      }
    })(input)
  }

  /**
    * Take a set of tiles, and transform the tile contents
    *
    * @param input The input data set of already tiled data
    * @tparam TC The tile coordinate type
    * @tparam BC The bin coordinate type
    * @tparam X The aggregators' coordinate type
    * @tparam VI The input value type
    * @tparam VO The output value type
    * @return A new set of tiles with the contents transformed as per the input function
    */
  def tileTransformationOp[TC, BC, X, VI, VO: ClassTag] (transformation: (VI, TC, BC) => VO)
                                                        (input: RDD[SeriesData[TC, BC, VI, X]]): RDD[SeriesData[TC, BC, VO, X]] = {
    input.map { data =>
      val tileCoordinate = data.coords
      val projection = data.projection
      val maxBin = data.maxBin

      val outputBins = data.bins.mapWithIndex { (value, index) =>
        val binCoordinate = projection.binFrom1D(index, maxBin)
        transformation(value, tileCoordinate, binCoordinate)
      }

      new SeriesData[TC, BC, VO, X](
        data.projection, maxBin, tileCoordinate, outputBins, data.tileMeta
      )
    }
  }
}
