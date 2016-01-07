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
package software.uncharted.xdata.sparkpipe

import java.io.File

import com.typesafe.config.{ConfigValueFactory, ConfigFactory}

import scala.collection.JavaConversions._ // scalastyle:ignore
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, BeforeAndAfter}
import software.uncharted.xdata.spark.SparkFunSpec

class GeoHeatmapGeneratorTest extends FunSpec {

  private val testOutputDir: String = "build/tmp/test_file_output/test_heatmap"

  describe("GeoHeatmapGeneratorTest") {
    describe("#execute") {
      it("should create tiles from source csv data with time filter applied") {
        SparkFunSpec.sparkLock.acquire()
        try {
          // run the job
          val path = classOf[GeoHeatmapGeneratorTest].getResource("/tiling-file-io.conf").toURI.getPath
          GeoHeatmapGenerator.execute(Array(path))

          val files = collectFiles
          val expected = Set(
            (0,0,0), // l0
            (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
            (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          SparkFunSpec.sparkLock.release()
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }

      it("should convert string date to timestamp") {
        SparkFunSpec.sparkLock.acquire()
        try {
          val path = classOf[GeoHeatmapGeneratorTest].getResource("/tiling-file-io.conf").toURI.getPath
          val config = ConfigFactory.parseReader(scala.io.Source.fromFile(path).bufferedReader())
          val dateConfig = config.withFallback(config.withValue("geoHeatmap.timeFormat", ConfigValueFactory.fromAnyRef("yyyy-MM-dd HH:mm:ss")))

          // run the job
          GeoHeatmapGenerator.execute(dateConfig)

          val files = collectFiles
          val expected = Set(
            (0,0,0), // l0
            (1,0,0), (1,1,0), (1,1,1), (1,0,1), // l1
            (2, 0, 0), (2, 2, 0), (2, 1, 1), (2, 3, 1), (2, 0, 2), (2, 2, 2), (2, 1, 3), (2, 3, 3)) // l2

          assertResult((Set(), Set()))((expected diff files, files diff expected))
        } finally {
          SparkFunSpec.sparkLock.release()
          FileUtils.deleteDirectory(new File(testOutputDir))
        }
      }
    }
  }

  def collectFiles: Set[(Int, Int, Int)] = {
    // convert produced filenames into indices
    val filename =
      """.*\\(\d+)\\(\d+)\\(\d+).bin""".r
    val files = FileUtils.listFiles(new File(testOutputDir), Array("bin"), true)
      .map(_.toString match { case filename(level, x, y) => (level.toInt, x.toInt, y.toInt) })
      .toSet
    files
  }
}
