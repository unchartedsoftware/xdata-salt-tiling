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

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import org.scalatest.FunSpec

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

      it("should run a LDA Augmentation with input and output separators that are different") {
        val inputData = getClass.getResource("LDAAugmentationTest.data").toURI.getPath
        val outputData = getEmptyTempFile("lda-augmentation", ".data")
        val configFile = modifyConfig(
          "LDAAugmentationTest-diffSeparator.conf",
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
