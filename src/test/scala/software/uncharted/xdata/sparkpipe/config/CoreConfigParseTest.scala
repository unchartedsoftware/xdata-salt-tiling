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
package software.uncharted.xdata.sparkpipe.config

import com.typesafe.config.{ConfigFactory, Config}
import org.scalatest.FunSpec
import software.uncharted.xdata.ops.io.S3Test

class CoreConfigParseTest() extends FunSpec {
  describe("S3OutputConfig") {
    describe("#apply") {
      it("should create an S3 config object from an input config", S3Test) {
        val config = ConfigFactory.parseString(
          """s3Output.awsAccessKey = ${AWS_ACCESS_KEY}
            |s3Output.awsSecretKey = ${AWS_SECRET_KEY}
            |s3Output.bucket = someBucket
            |s3Output.layer = someLayer
            |s3Output.ext = bin""".stripMargin).resolve()

        val accessKey = sys.env("AWS_ACCESS_KEY")
        val secretKey = sys.env("AWS_SECRET_KEY")
        assertResult(S3OutputConfig(accessKey, secretKey, "someBucket", "someLayer", "bin"))(S3OutputConfig(config).get)
      }
    }
  }
}
