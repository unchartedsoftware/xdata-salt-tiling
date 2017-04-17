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

package software.uncharted.xdata.tiling.jobs

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.uncharted.xdata.ops.salt.text.{DictionaryConfiguration, DictionaryConfigurationParser, LDAOp}
import software.uncharted.xdata.tiling.config.{HdfsCsvConfig, HdfsCsvConfigParser, LDAConfig}

import scala.util.{Failure, Success}

/**
  * A job that augments a csv-like dataset with a new column representing the LDA-derived topics in that dataset
  */
object LDAAugmentationJob extends AbstractJob {
  private def readInputConfig (config: Config): HdfsCsvConfig = {
    HdfsCsvConfigParser.parse("input")(config) match {
      case Success(c) =>
        if (c.neededColumns.length != 1) {
          error("Input configuration specifies other than 1 column")
          sys.exit(-1)
        }
        c
      case Failure(e) =>
        error("Error reading input config", e)
        sys.exit(-1)
    }
  }

  private def readOutputConfig (config: Config): HdfsCsvConfig = {
    HdfsCsvConfigParser.parse("output")(config) match {
      case Success(c) => c
      case Failure(e) =>
        error("Error reading output configuration", e)
        sys.exit(-1)
    }
  }

  private def readLDAConfig (config: Config): LDAConfig = {
    LDAConfig.parse(config) match {
      case Success(c) => c
      case Failure(e) =>
        error("Error reading LDA configuration", e)
        sys.exit(-1)
    }
  }
  private def readDictionaryConfig (config: Config): DictionaryConfiguration = {
    DictionaryConfigurationParser.parse(config)
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    config.resolve()
    // Ignore info messages
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

    val inputConfig = readInputConfig(config)
    val outputConfig = readOutputConfig(config)
    val ldaConfig = readLDAConfig(config)
    val dictionaryConfig = readDictionaryConfig(config)

    // Read data
    val inputData = readFile(session.sparkContext, inputConfig).zipWithIndex().map { case ((rawRecord, fields), index) =>
      val text = fields.head
      (index, (rawRecord, text))
    }

    // Pull out the text column
    val texts = inputData.map { case (id, (rawRecord, text)) => (id, text) }
    dbg("(1) There are " + texts.count + " texts")
    // Perform LDA on the text column
    val docTopics = LDAOp.textLDATopics[(Long, String)](dictionaryConfig, ldaConfig, _._2, _._1)(texts)
    dbg("(2) There are " + docTopics.count + " topic records")
    // Reformat topics for output
    val formattedTopics = docTopics.map { case (docId, topics) =>
      (
        docId,
        topics.map { entry =>
          entry.topic.map(wordScore => wordScore.word + ldaConfig.scoreSeparator + wordScore.score)
            .mkString(ldaConfig.wordSeparator) + ldaConfig.wordSeparator + entry.score
        }.mkString(ldaConfig.topicSeparator)
        )
    }
    dbg("(3) There are " + formattedTopics.count + " formatted topic records")
    // Join the LDA results back in
    val joinedData = inputData.join(formattedTopics)
    dbg("(4) There are " + joinedData.count + " joined records")
    // TODO: Sort or not based upon an input parameter
    val sortedData = joinedData.sortBy(_._1)
    dbg("(5) There are " + sortedData.count + " sorted recoreds")
    val output = sortedData.map { case (id, ((rawRecord, text), topics)) =>
      rawRecord + inputConfig.separator + topics
    }

    // Replace separators if necessary
    val toWrite = reformatOutput(output, inputConfig, outputConfig)
    dbg("(6) There are " + toWrite.count + " records to write\n" + "Writing to " + outputConfig.location)
    // Write out the data
    toWrite.saveAsTextFile(outputConfig.location)
  }

  private def reformatOutput (startingOutput: RDD[String],
                              inputConfig: HdfsCsvConfig,
                              outputConfig: HdfsCsvConfig): RDD[String] = {
    val reseparated =
      if (outputConfig.separator == inputConfig.separator) {
        startingOutput
      } else {
        startingOutput.map(line => line.split(inputConfig.separator).mkString(outputConfig.separator))
      }
    outputConfig.partitions.map(partitions => reseparated.repartition(partitions)).getOrElse(reseparated)
  }

  // scalastyle:off regex
  def dbg (msg: String): Unit = {
    println
    println
    println
    println(msg)
    println
    println
    println
  }
  // scalastyle:on regex

  def readFile (sc: SparkContext, config: HdfsCsvConfig): RDD[(String, Seq[String])] = {
    sc.textFile(config.location).map{line =>
      val fields = line.split(config.separator)
      val relevantFields = config.neededColumns.map(n => fields(n))
      (line, relevantFields)
    }
  }
}
