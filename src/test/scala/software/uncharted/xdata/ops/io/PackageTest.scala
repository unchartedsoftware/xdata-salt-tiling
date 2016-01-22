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
import java.nio.file.{Paths, Files}

import org.apache.commons.io.FileUtils
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.xdata.ops.salt.MercatorTimeProjection
import software.uncharted.xdata.spark.SparkFunSpec

import scala.util.parsing.json.JSONObject

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals
class PackageTest extends SparkFunSpec {

  private val testDir = "/test_data"
  private val testLayer = "test_layer"
  private val testBucket = "uncharted-s3-client-test"
  private val extension = "tst"

  describe("#writeToFile") {
    it("should create the folder directory structure if it's missing") {
      try {
        val data = sc.parallelize(Seq(((1, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
        writeToFile(s"${testDir}_1", testLayer, extension)(data)
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
        writeToFile(s"${testDir}_2", testLayer, extension)(data)
        assertResult(true)(new File(s"${testDir}_2/$testLayer/2/2/2.tst").exists())
        assertResult(true)(new File(s"${testDir}_2/$testLayer/2/2/3.tst").exists())
      } finally {
        FileUtils.deleteDirectory(new File(s"${testDir}_2"))
      }
    }

    it("should serialize the byte array without changing it") {
      try {
        val data = sc.parallelize(Seq(((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
        writeToFile(s"${testDir}_3", testLayer, extension)(data)
        val bytes = Files.readAllBytes(Paths.get(s"${testDir}_3/$testLayer/2/2/2.tst"))
        assertResult(Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))(bytes)
      } finally {
        FileUtils.deleteDirectory(new File(s"${testDir}_3"))
      }
    }
  }

  describe("#writeToS3") {
    val awsAccessKey = sys.env("AWS_ACCESS_KEY")
    val awsSecretKey = sys.env("AWS_SECRET_KEY")
    val testKey0 = s"$testLayer/2/2/2.bin"
    val testKey1 = s"$testLayer/2/2/3.bin"

    it("should add tiles to the s3 bucket using key names based on TMS coords") {
      val data = sc.parallelize(Seq(
        ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
        ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
      ))

      writeToS3(awsAccessKey, awsSecretKey, testBucket, testLayer)(data).collect()

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

      writeToS3(awsAccessKey, awsSecretKey, testBucket, testLayer)(data)
      val s3c = new S3Client(awsAccessKey, awsSecretKey)
      assertResult(Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))(s3c.download(testBucket, testKey0).getOrElse(fail()))
      s3c.delete(testBucket, testKey0)
    }
  }

  describe("#serializeBinArray") {
    it("should create an RDD of bin coordinate / byte array tuples from series data") {
      val series = sc.parallelize(
        Seq(
          new SeriesData[(Int, Int, Int), Double, (Double, Double)]
          ((1, 2, 3), Seq(0.0, 1.0, 2.0, 3.0), 4, 0.0, None, new MercatorTimeProjection()),
          new SeriesData[(Int, Int, Int), Double, (Double, Double)]
          ((4, 5, 6), Seq(4.0, 5.0, 6.0, 7.0), 4, 0.0, None, new MercatorTimeProjection())
        ))
      val result = serializeBinArray(series).collect()
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

  describe("#serializeBinArray") {
    it("should ignore tiles with no updated bins") {
      val series = sc.parallelize(
        Seq(new SeriesData[(Int, Int, Int), Double, (Double, Double)]
          ((1, 2, 3), Seq(0.0, 1.0, 2.0, 3.0), 0, 0.0, None, new MercatorTimeProjection())))
      val result = serializeBinArray(series).collect()
      assertResult(0)(result.length)
    }
  }

  describe("#serializeElementScore") {
    it("should create an RDD of JSON strings serialized to bytes from series data ") {
      val series = sc.parallelize(
        Seq(
          new SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]((1, 2, 3), Seq(List("aa" -> 1, "bb" -> 2)), 1, List(), None, new MercatorTimeProjection()),
          new SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]((4, 5, 6), Seq(List("cc" -> 3, "dd" -> 4)), 1, List(), None, new MercatorTimeProjection())
        ))

      val json = List(new JSONObject(Map("aa" -> 1, "bb" -> 2)).toString().getBytes, new JSONObject(Map("cc" -> 3, "dd" -> 4)).toString().getBytes)

      val result = serializeElementScore(series).collect()
      assertResult(2)(result.length)

      assertResult(result(0)._1)((1, 2, 3))
      assertResult(json.head)(result(0)._2)

      assertResult(result(1)._1)((4, 5, 6))
      assertResult(json(1))(result(1)._2)
    }
  }

  describe("#serializeElementScore") {
    it("should ignore tiles with no updated bins ") {
      val series = sc.parallelize(
        Seq(new SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]((1, 2, 3), Seq(List("aa" -> 1, "bb" -> 2)), 0, List(), None, new MercatorTimeProjection())))
      val result = serializeElementScore(series).collect()
      assertResult(0)(result.length)
    }
  }
}
