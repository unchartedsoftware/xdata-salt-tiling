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
package software.uncharted.xdata.sparkpipe.jobs

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.Tag
import net.liftweb.json._
import scala.collection.JavaConversions._ // scalastyle:ignore

object FileIOTest extends Tag("fileio.test")

/**
  * Set of utility functions for testing jobs
  */
object JobTestUtils {
  def collectFiles(rootDir: String, suffix: String) : Set[(Int, Int, Int)] = {
    // convert produced filenames into indices
    val filename = s""".*[/\\\\](\\d+)[/\\\\](\\d+)[/\\\\](\\d+)\\.$suffix""".r
    val files = FileUtils.listFiles(new File(rootDir), Array(suffix), true)
      .flatMap(_.toString match {
        case filename(level, x, y) => Some((level.toInt, x.toInt, y.toInt))
        case _ => None
      })
      .toSet
    files
  }

  /**
    * Get coordinates (of all zoom levels) and their corresponding bin values
    *
    * @param rootDir Directory of generated tile data
    * @param suffix File suffix for extraction of data
    * @return List of tile coordinates and their bin values
    */
  def getBinValues(rootDir: String, suffix: String): List[((Int, Int, Int), JValue)] = {
    val filename = s""".*[/\\\\](\\d+)[/\\\\](\\d+)[/\\\\](\\d+)\\.$suffix""".r
    val results = FileUtils.listFiles(new File(rootDir), Array(suffix), true)
      .map { inputFile =>
        val byteAra = FileUtils.readFileToByteArray(inputFile)
        val binValue = new String(byteAra)
        val coord = Array(inputFile).flatMap(_.toString match {
            case filename(level, x, y) => Some((level.toInt, x.toInt, y.toInt))
            case _ => None
          }).head
        (coord, parse(binValue))
      }.toList
    results
  }
}
