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

import net.liftweb.json._
import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

class XYSegmentJobTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/segment"
  private val suffix: String = "bin"

  describe("XYSegmentJobTest") {
    // scalastyle:ignore
    import net.liftweb.json.JsonDSL._
    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val path = classOf[XYSegmentJobTest].getResource("/tiling-file-io-xysegment.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val newDir = path.substring(0, path.indexOf("xdata-pipeline-ops") + 18)
          System.setProperty("user.dir", newDir)
          XYSegmentJob.main(Array(path))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          val expected = Set(
            (0,0,0),
            (1,0,1), (1,1,0),
            (2,1,2), (2,2,1),
            (3,2,5), (3,3,4), (3,3,5), (3,4,3),
            (4,4,11),
            (5,9,23),
            (6,18,47),
            (7,37,94),
            (8,75,189),
            (9,150,378), (9,150,379), (9,151,378),
            (10,301,756), (10,301,757), (10,301,758), (10,302,756), (10,302,757),
            (11,602,1513), (11,602,1514), (11,603,1512), (11,603,1513), (11,603,1514), (11,603,1515), (11,603,1516), (11,604,1512), (11,604,1513), (11,604,1514), (11,604,1515),
            (12,1204,3027), (12,1204,3028), (12,1205,3028), (12,1205,3029), (12,1206,3026), (12,1206,3027), (12,1206,3028), (12,1206,3029), (12,1206,3030), (12,1206,3031), (12,1206,3032), (12,1207,3025), (12,1207,3026), (12,1207,3027), (12,1207,3028), (12,1207,3029), (12,1207,3030), (12,1207,3031), (12,1207,3032), (12,1208,3025), (12,1208,3026), (12,1208,3027), (12,1208,3028), (12,1208,3029), (12,1208,3030),
            (13,2411,6056), (13,2411,6057), (13,2411,6058), (13,2412,6053), (13,2412,6054), (13,2412,6055), (13,2412,6056), (13,2412,6057), (13,2412,6058), (13,2412,6059), (13,2412,6060), (13,2412,6061), (13,2413,6055), (13,2413,6056), (13,2413,6057), (13,2413,6058), (13,2413,6059), (13,2413,6060), (13,2413,6061), (13,2413,6062), (13,2413,6063), (13,2413,6064), (13,2414,6056), (13,2414,6057), (13,2414,6058), (13,2414,6059), (13,2415,6050), (13,2415,6051), (13,2415,6057), (13,2415,6058), (13,2415,6059), (13,2416,6051), (13,2416,6052), (13,2416,6053), (13,2416,6058), (13,2417,6053)
          )

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
//          System.setProperty("user.dir", oldDir)
//          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
