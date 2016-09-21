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
import org.scalatest.FunSpec

// scalastyle:off multiple.string.literals
class TopicModellingJobTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/test_topic_modelling"
  private val suffix: String = "bin"

  describe("TopicModellingJobTest") {
    describe("#execute") {
      it("should do topic modelling", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  To fix this we reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards.
        val oldDir = System.getProperty("user.dir")
        try {
          val path = classOf[TopicModellingJobTest].getResource("/topic-modelling/topic-modelling-parallel.conf").toURI.getPath
          // Run the test from path/to/xdata-pipeline-ops/
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)

          TopicModellingJob.main(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), (1, 1, 0), (1, 1, 1), (1, 0, 1), // l1
            (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))

          // check metadata
//          val fileStr = FileUtils.readFileToString(new File(s"$testOutputDir/metadata.json"))
//          val jsonObject = parse(fileStr)
//          val expectedJson =
//            ("bins" -> 4) ~
//              ("range" ->
//                (("start" -> 1357016400000L) ~
//                  ("step" -> 86400000) ~
//                  ("count" -> 8)))
//          assertResult(expectedJson)(jsonObject)

        } finally {
          System.setProperty("user.dir", oldDir)
//          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
