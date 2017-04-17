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

package software.uncharted.xdata.tiling.config

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec


class HdfsCsvConfigParserTest extends FunSpec {
  describe("HdfsCsvConfigParser") {
    describe("#parse") {
      it("should read an HDFS CSV configuration with the proper syntax") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    location: "hdfs://user/foobar/test-file.xyz"
            |    partitions: 4
            |    separator: "\t"
            |    columns: [3, 4, 8]
            |}
          """.stripMargin).resolve()
        val hdfsConfig = HdfsCsvConfigParser.parse("foobar")(config).get

        assert("hdfs://user/foobar/test-file.xyz" === hdfsConfig.location)
        assert(4 === hdfsConfig.partitions.get)
        assert("\t" === hdfsConfig.separator)
        assert(List(3, 4, 8) === hdfsConfig.neededColumns.toList)
      }

      it("should correctly distinguish two HDFS CSV configurations") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    xyzzy {
            |        location: "hdfs://user/foobar/test-file.xyz"
            |    }
            |    plover {
            |        location: "hdfs://another/random/location.txt"
            |    }
            |}
          """.stripMargin).resolve()
        val xyzzy = HdfsCsvConfigParser.parse("foobar.xyzzy")(config).get
        val plover = HdfsCsvConfigParser.parse("foobar.plover")(config).get

        assert("hdfs://user/foobar/test-file.xyz" === xyzzy.location)
        assert("hdfs://another/random/location.txt" === plover.location)
      }

      it("should not supply a number of partitions if none is given") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    location: "hdfs://user/foobar/test-file.xyz"
            |}
          """.stripMargin).resolve()
        val hdfsConfig = HdfsCsvConfigParser.parse("foobar")(config).get

        assert(hdfsConfig.partitions.isEmpty)
      }

      it("should supply a default field separator as specified, if not present in the config file") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    location: "hdfs://user/foobar/test-file.xyz"
            |    partitions: 4
            |}
          """.stripMargin).resolve()
        val hdfsConfig = HdfsCsvConfigParser.parse("foobar", "def-sep")(config).get

        assert("def-sep" === hdfsConfig.separator)
      }

      it("should not supply a default field separator as specified, if one is present in the config file") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    location: "hdfs://user/foobar/test-file.xyz"
            |    partitions: 4
            |    separator: "\t"
            |}
          """.stripMargin).resolve()
        val hdfsConfig = HdfsCsvConfigParser.parse("foobar", "def-sep")(config).get

        assert("\t" === hdfsConfig.separator)
      }

      it("Should supply an empty list of columns if no columns are specified") {
        val config = ConfigFactory.parseString(
          """
            |foobar {
            |    location: "hdfs://user/foobar/test-file.xyz"
            |    partitions: 4
            |}
          """.stripMargin).resolve()
        val hdfsConfig = HdfsCsvConfigParser.parse("foobar")(config).get

        assert(List[Int]() === hdfsConfig.neededColumns.toList)
      }
    }
  }

}
