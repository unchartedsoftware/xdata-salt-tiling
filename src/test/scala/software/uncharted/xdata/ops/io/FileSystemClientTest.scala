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
package software.uncharted.xdata.ops.io

import java.io.File

import org.apache.commons.io.FileUtils
import software.uncharted.sparkpipe.ops.xdata.io.{FileSystemClient, mkRowId}
import software.uncharted.xdata.spark.SparkFunSpec

class FileSystemClientTest extends SparkFunSpec {
  private val testDir = "build/tmp/test_file_output/test_data"
  private val testLayer = "test_layer"
  private val extension = ".tst"

  describe("read and write tests") {
    it ("should write and read the same data") {
      try {
        val fsc = new FileSystemClient(testDir, Some(extension))

        val data = "hello world".getBytes
        val coordinates = (1, 2, 3)
        val dataLocation = mkRowId("", "/", "")(coordinates._1, coordinates._2, coordinates._3)

        fsc.writeRaw(testLayer, s"$dataLocation$extension", data)
        val result = fsc.readRaw(s"$testLayer/$dataLocation").get

        assertResult(data)(result)
      } finally {
        FileUtils.deleteDirectory(new File(testDir))
      }
    }
  }

}
