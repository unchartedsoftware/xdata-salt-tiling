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

import org.scalatest.FunSpec
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import software.uncharted.sparkpipe.ops.xdata.io.S3Test

class CoreConfigParseTest() extends FunSpec {
  describe("SparkConfig") {
    describe("#applySparkConfigEntries") {
      it("should apply entries from multiple direct and indirect levels") {
        val config = ConfigFactory.parseString(
          """
            |spark {
            |    app.name = "test application"
            |    executor {
            |        instances = 8
            |        cores = 4
            |	       memory = 10g
            |    }
            |    network.timeout = 900
            |}
            |
          """.stripMargin
        )
        val conf = SparkConfig.applySparkConfigEntries(config)(new SparkConf())
        assert(8 === conf.getInt("spark.executor.instances", -1))
        assert(4 === conf.getInt("spark.executor.cores", -1))
        assert("10g" === conf.get("spark.executor.memory"))
        assert("test application" === conf.get("spark.app.name"))
        assert(900 === conf.getInt("spark.network.timeout", -1))
      }
      it("should set the checkpoint directory since the config key is set") {
        val config = ConfigFactory.parseString(
          """
            |spark {
            |    app.name = "test application"
            |    executor {
            |        instances = 8
            |        cores = 4
            |	       memory = 10g
            |    }
            |    network.timeout = 900
            |    checkpoint-directory = "build/tmp"
            |}
            |
          """.stripMargin
        )
        val conf = SparkConfig.applySparkConfigEntries(config)(new SparkConf())
        assert("build/tmp" === conf.get("spark.checkpoint-directory"))
      }
    }
  }

  describe("S3OutputConfig") {
    describe("#parse") {
      it("should create an S3 config object from an input config with proper syntax", S3Test) {
        val config = ConfigFactory.parseString(
          """s3Output.awsAccessKey = ${?AWS_ACCESS_KEY}
            |s3Output.awsSecretKey = ${?AWS_SECRET_KEY}
            |s3Output.bucket = someBucket
            |s3Output.layer = someLayer
            |s3Output.ext = bin""".stripMargin).resolve()

        val accessKey = sys.env("AWS_ACCESS_KEY")
        val secretKey = sys.env("AWS_SECRET_KEY")
        assertResult(S3OutputConfig(accessKey, secretKey, "someBucket", "someLayer", "bin"))(S3OutputConfig.parse(config).get)
      }

      it("should create an S3 config object from an input config with legacy environment variable syntax", S3Test) {
        val config = ConfigFactory.parseString(
          """s3Output.awsAccessKey = ${AWS_ACCESS_KEY}
            |s3Output.awsSecretKey = ${AWS_SECRET_KEY}
            |s3Output.bucket = someBucket
            |s3Output.layer = someLayer
            |s3Output.ext = bin""".stripMargin).resolve()

        val accessKey = sys.env("AWS_ACCESS_KEY")
        val secretKey = sys.env("AWS_SECRET_KEY")
        assertResult(S3OutputConfig(accessKey, secretKey, "someBucket", "someLayer", "bin"))(S3OutputConfig.parse(config).get)
      }

      it("should use the default extension", S3Test) {
        val config = ConfigFactory.parseString(
          """s3Output.awsAccessKey = ${?AWS_ACCESS_KEY}
            |s3Output.awsSecretKey = ${?AWS_SECRET_KEY}
            |s3Output.bucket = someBucket
            |s3Output.layer = someLayer""".stripMargin).resolve()

        val accessKey = sys.env("AWS_ACCESS_KEY")
        val secretKey = sys.env("AWS_SECRET_KEY")
        assertResult(S3OutputConfig(accessKey, secretKey, "someBucket", "someLayer", "bin"))(S3OutputConfig.parse(config).get)
      }
    }
  }

  describe("HBaseOutputConfig") {
    describe("#parse") {
      it ("should create a hbase config object") {
        val configFile = Seq(classOf[CoreConfigParseTest].getResource("/hbase-site.xml").toURI.getPath)

        val config = ConfigFactory.parseString(
          s"""hbaseOutput.configFiles = ["$configFile"]
             |hbaseOutput.layer = test_layer
             |hbaseOutput.qualifier = ""
          """.stripMargin).resolve()

        val result = HBaseOutputConfig.parse(config).get
        val expected = HBaseOutputConfig(Seq(configFile.toString), "test_layer", "")

        assertResult(expected)(result)
      }
    }
  }

}
