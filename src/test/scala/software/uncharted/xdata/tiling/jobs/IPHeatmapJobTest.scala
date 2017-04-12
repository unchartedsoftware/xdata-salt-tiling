package software.uncharted.xdata.tiling.jobs

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.FunSpec

/**
  * Created by nkronenfeld on 25/10/16.
  */
class IPHeatmapJobTest extends FunSpec {
  private val testOutputDir: String = "build/tmp/test_file_output/test_ip_heatmap"
  private val suffix: String = "bin"

  describe("IPHeatmapJobTest") {
    describe("#execute") {
      it("should create tiles from source csv data", FileIOTest) {
        // When test are run from another project that includes this project, the current working directory is set such
        // that the data files referenced in tiling-file-io.conf can't be found.  We reset the CWD to the
        // xdata-pipeline-ops directory, and reset it afterwards, to get around this problem.
        val oldDir = System.getProperty("user.dir")
        try {
          // run the job
          val config = classOf[IPHeatmapJobTest].getResource("/IPHeatmapJobTest/tiling-ip.conf").toURI.getPath
          // Make sure to run the test from the correct directory
          val project = "xdata-pipeline-ops"
          val newDir = config.substring(0, config.indexOf(project) + project.length)
          System.setProperty("user.dir", newDir)
          IPHeatmapJob.execute(Array(config))

          val files = JobTestUtils.collectFiles(testOutputDir, suffix)
          // Data should be the diagonal line in the upper left quadrant only.
          val expected = Set(
            (0, 0, 0), // l0
            (1, 0, 0), // l1
            (2, 0, 0), (2, 1, 1) // l2
          )

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          System.setProperty("user.dir", oldDir)
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }
}
