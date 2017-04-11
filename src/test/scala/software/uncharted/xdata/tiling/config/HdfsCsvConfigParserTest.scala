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
