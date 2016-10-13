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

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec
import scala.io.Source.fromFile

// scalastyle:off multiple.string.literals
class TopicModellingJobTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/test_heatmap"
  private val suffix: String = "bin"

  describe("TopicModellingParallelJobTest") {
    // When test are run from another project that includes this project, the current working directory is set such
    // that the data files referenced in tiling-file-io.conf can't be found.  To fix this we reset the CWD to the
    // xdata-pipeline-ops directory, and reset it afterwards.
    val oldDir = System.getProperty("user.dir")
    try {
      val path = classOf[TopicModellingJobTest].getResource("/topic-modelling/topic-modelling.conf").toURI.getPath
      // Run the test from path/to/xdata-pipeline-ops/
      val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
      System.setProperty("user.dir", newDir)

      describe("#main()") {

        it("should run the topic modelling job") {
          TopicModellingJob.main(Array(path))
        }

        val expected = fromFile("src/test/resources/topic-modelling/results/expected/part-00000").getLines
        val actual = fromFile("src/test/resources/topic-modelling/results/actual/part-00000").getLines

        it("should have an output that has the same number of rows as the expected output"){
          assert(expected.length == actual.length)
        }

        it("should have output that matches the expected output exactly") {
          assert((expected zip actual).forall(x => x._1 == x._2))
        }
      }
    } finally {
      System.setProperty("user.dir", oldDir)
    }
  }
}
