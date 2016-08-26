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

import scala.collection.JavaConversions._ // scalastyle:ignore


object FileIOTest extends Tag("fileio.test")

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
}
