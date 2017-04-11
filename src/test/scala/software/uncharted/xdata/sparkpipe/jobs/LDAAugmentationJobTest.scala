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
import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSpec
import software.uncharted.xdata.tiling.jobs.LDAAugmentationJob

import scala.io.Source

class LDAAugmentationJobTest extends FunSpec {
  describe("LDA Augmentation job") {
    describe("#execute") {
      it("should validate a LDA Augmentation job") {
        val inputData = getClass.getResource("LDAAugmentationTest.data").toURI.getPath
        val outputData = getEmptyTempFile("lda-augmentation", ".data")
        val configFile = modifyConfig(
          "LDAAugmentationTest.conf",
          ("INPUT", inputData),
          ("OUTPUT", outputData)
        )
        new File(configFile).deleteOnExit()
        LDAAugmentationJob.execute(Array(configFile))

        val outputFile = new File(outputData)
        outputFile.deleteOnExit()
        assert(outputFile.exists())
        assert(outputFile.isDirectory)
        val children = outputFile.listFiles()
        assert(4 === children.size)
        assert(children.map(_.getName).toSet.contains("_SUCCESS"))
        assert(children.map(_.getName).toSet.contains("._SUCCESS.crc"))
        assert(children.map(_.getName).toSet.contains("part-00000"))
        assert(children.map(_.getName).toSet.contains(".part-00000.crc"))
        val output = Source.fromFile(new File(outputFile, "part-00000")).getLines().toArray
        assert(20 === output.length)
      }
    }
  }

  def getEmptyTempFile (prefix: String, suffix: String): String = {
    val tmpFile = File.createTempFile(prefix, suffix)
    tmpFile.delete()
    tmpFile.getAbsolutePath
  }

  def modifyConfig (inputLocation: String, replacements: (String, String)*): String = {
    val configFile = File.createTempFile("lda-augmentation", ".conf")

    val input = Source.fromInputStream(getClass().getResourceAsStream(inputLocation)).getLines

    val output = new OutputStreamWriter(new FileOutputStream(configFile))

    input.foreach { line =>
      var fixedLine = line
      replacements.foreach{case (from, to) =>
          fixedLine = fixedLine.replaceAll(from, to)
      }
      output.write(fixedLine)
      output.write("\n")
    }
    output.flush()
    output.close()

    configFile.getAbsolutePath
  }
}
