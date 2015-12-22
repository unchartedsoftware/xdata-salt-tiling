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
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.xdata.ops.salt.MercatorTimeProjection
import software.uncharted.xdata.spark.SparkFunSpec

// scalastyle:off magic.number

class BinArraySerializerOpTest extends SparkFunSpec {

  private val testDir = "./test_data"
  private val testLayer = "test_layer"

  private val testBucket = "uncharted-s3-client-test"
  val tempTestBucket = "uncharted-s3-client-test-temp"

  describe("BinArraySerializer") {

    describe("#binArraySerializerOp") {
      it("should create an RDD of bin coordinate / byte array tuples from series data") {
        val series = sc.parallelize(
          Seq(
            new SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]
            ((1, 2, 3), Seq(0.0, 1.0, 2.0, 3.0), 4, 0.0, None, new MercatorTimeProjection()),
            new SeriesData[(Int, Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]
            ((4, 5, 6), Seq(4.0, 5.0, 6.0, 7.0), 4, 0.0, None, new MercatorTimeProjection())
          ))
        val result = BinArraySerializerOp.binArraySerializeOp(series).collect()
        assertResult(2)(result.length)
        assertResult(result(0)._1)((1, 2, 3))
        assertResult(result(0)._2)(
          List(
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, -16, 63,
            0, 0, 0, 0, 0, 0, 0, 64,
            0, 0, 0, 0, 0, 0, 8, 64))

        assertResult(result(1)._1)((4, 5, 6))
        assertResult(result(1)._2)(
          List(
            0, 0, 0, 0, 0, 0, 16, 64,
            0, 0, 0, 0, 0, 0, 20, 64,
            0, 0, 0, 0, 0, 0, 24, 64,
            0, 0, 0, 0, 0, 0, 28, 64))
      }
    }

    describe("#binArrayFileStoreOp") {

      it("should create the folder directory structure if it's missing") {
        try {
          val data = sc.parallelize(Seq(((1, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
          BinArraySerializerOp.binArrayFileStoreOp(s"${testDir}_1", testLayer)(data)
          val testFile = new File(s"${testDir}_1/$testLayer/1/2")
          assertResult(true)(testFile.exists())
        } finally {
          FileUtils.deleteDirectory(new File(s"${testDir}_1"))
        }
      }

      it("should add tiles to the folder hierarchy using a path based on TMS coords ") {
        try {
          val data = sc.parallelize(Seq(
            ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
            ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
          ))
          BinArraySerializerOp.binArrayFileStoreOp(s"${testDir}_2", testLayer)(data)
          assertResult(true)(new File(s"${testDir}_2/$testLayer/2/2/2.bin").exists())
          assertResult(true)(new File(s"${testDir}_2/$testLayer/2/2/3.bin").exists())
        } finally {
          FileUtils.deleteDirectory(new File(s"${testDir}_2"))
        }
      }

      it("should serialize the byte without changing it") {
        try {
          val data = sc.parallelize(Seq(((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
          BinArraySerializerOp.binArrayFileStoreOp(s"${testDir}_3", testLayer)(data)
          val bytes = Files.readAllBytes(Paths.get(s"${testDir}_3/$testLayer/2/2/2.bin"))
          assertResult(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))(bytes)
        } finally {
          FileUtils.deleteDirectory(new File(s"${testDir}_3"))
        }
      }
    }

    describe("#binArrayS3StoreOp") {
      val awsAccessKey = sys.env("AWS_ACCESS_KEY")
      val awsSecretKey = sys.env("AWS_SECRET_KEY")
      val testKey0 = s"$testLayer/2-2-2.bin"
      val testKey1 = s"$testLayer/2-2-3.bin"

      it("should add tiles to the s3 bucket using key names based on TMS coords") {
        val data = sc.parallelize(Seq(
          ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
          ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
        ))

        BinArraySerializerOp.binArrayS3StoreOp(awsAccessKey, awsSecretKey,
          testBucket, testLayer)(data).collect()

        val s3c = new S3Client(awsAccessKey, awsSecretKey)
        assert(s3c.download(testBucket,testKey0).isDefined)
        assert(s3c.download(testBucket, testKey1).isDefined)
        s3c.delete(testBucket, testKey0)
        s3c.delete(testBucket, testKey1)
      }

      it("should serialize the byte data to the s3 bucket without changing it") {
        val data = sc.parallelize(Seq(
          ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
        ))

        BinArraySerializerOp.binArrayS3StoreOp(awsAccessKey, awsSecretKey,
          testBucket, testLayer)(data)
        val s3c = new S3Client(awsAccessKey, awsSecretKey)
        assertResult(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))(s3c.download(testBucket, testKey0).getOrElse(fail()))
        s3c.delete(testBucket, testKey0)
      }
    }
  }
}
