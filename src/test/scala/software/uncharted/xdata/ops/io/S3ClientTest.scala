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

import org.scalatest.{BeforeAndAfterAll, FunSpec, Tag}
import java.io.ByteArrayInputStream

object S3Test extends Tag("s3.test")

class S3ClientTest extends FunSpec with BeforeAndAfterAll {

  private val testBucket = "uncharted-s3-client-test"
  private val nonexistentBucket = "nonexistent-bucket"
  val tempTestBucket = "uncharted-s3-client-test-temp"

  private val test0 = "folder/test0.bin"
  private val test1 = "folder/test1.bin"
  private val test2 = "folder/test2.bin"
  private val test3 = "folder/test3.bin"

  private val test4 = "folder/test4.bin"
  private val test5 = "folder/test5.bin"
  private val test6 = "folder/test6.bin"
  private val test7 = "folder/test7.bin"

  private val data = Array[Byte](0, 1, 2, 3, 4, 5)
  private val data_dl = Array[Byte](0, 11, 22, 33, 44, 55) // scalastyle:ignore

  private val zipped_data = Array[Byte](31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 100, 98, 102, 97, 5, 0, 74, -49, -21, 48, 6, 0, 0, 0)
  private val zipped_zipped_data = Array[Byte](31, -117, 8, 0, 0, 0, 0, 0, 0, 0, -109, -17, -26, 96, -128, -128, -28, -124, -108, -92, -76, 68, 86, 6, -81, -13, -81, 13, -40, -128, 124, 0, -128, -2, 33, -115, 26, 0, 0, 0)


  private val zipped_data_input_stream = new ByteArrayInputStream(zipped_data)
  private val unzipped_data_input_stream = new ByteArrayInputStream(data)

  private lazy val s3 = S3Client(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))


  protected override def beforeAll() = {
    s3.createBucket(testBucket)
  }


  protected override def afterAll() = {
    s3.delete(testBucket, test0)
    s3.delete(testBucket, test1)
    s3.delete(testBucket, test2)
    s3.deleteBucket(tempTestBucket)
  }

  describe("S3ClientTest") {
    describe("#upload") {
      it("should return true if upload was successful", S3Test) {
        val result = s3.upload(data, testBucket, test0)
        assertResult(true)(result)
      }

      it("should return false if upload was unsuccessful", S3Test) {
        val result = s3.upload(data, nonexistentBucket, test0)
        assertResult(false)(result)
      }

      it("should create a gzipped S3 object with the supplied bucket, key and data and compressed unspecified", S3Test) {
        s3.upload(data, testBucket, test1)
        val result = s3.download(testBucket, test1, compress = false).getOrElse(fail("Failed to fetch uploaded data"))
        assertResult(zipped_data)(result)
      }

      it("should create an S3 object that is not gzipped if compress parameter is set to false", S3Test) {
        s3.upload(data, testBucket, test4, compress = false)
        val result = s3.download(testBucket, test4, false).getOrElse(fail("Failed to fetch uploaded data"))
        assertResult(data)(result)
      }

      it ("should create an S3 object with the correct object meta-data corresponding to specified mime-type", S3Test) {
        val result = s3.upload(data, testBucket, test7, "application/json")
        assertResult(true)(result)
      }
    }

    describe("#download") {
      it("should return decompressed data corresponding to bucket, key with compress parameter unspecified", S3Test) {
        s3.upload(data_dl, testBucket, test2)
        val result = s3.download(testBucket, test2).getOrElse(fail("Failed to download data"))
        assertResult(data_dl)(result)
      }

      it("should return the uploaded data without decompression, corresponding to bucket, key when compress is false", S3Test) {
        s3.upload(data, testBucket, test6)
        val result = s3.download(testBucket, test6, false).getOrElse(fail("Failed to download data"))
        assertResult(zipped_data)(result)
      }

      it("should return None if no data exists for bucket, key", S3Test) {
        val result = s3.download(nonexistentBucket, test1)
        assertResult(None)(result)
      }
    }

    describe("#zipData") {
      it ("should return Some array of bytes compressed using a GZIP byte filter", S3Test) {
        val result = s3.zipData(data).getOrElse(fail("Failed to zip data"))
        assertResult(zipped_data)(result)
      }
      //Apperently you can zip a zipped byte array too, so i'm not quite sure how to test this.
      // it ("should return None on GZIP failure", S3Test) {
      //   //
      // }

      it ("should return an compressed array of compressed bytes compressed when a compressed byte array is passed in", S3Test) {
        val result = s3.zipData(zipped_data).getOrElse(fail("Failed to zip data"))
        assertResult(zipped_zipped_data)(result)
      }
    }

    describe("#unzipData") {
      it ("should return Some array of decompressed bytes by passing an input stream with bytes", S3Test) {
        val result = s3.unzipData(zipped_data_input_stream).getOrElse(fail("Failed to unzip data"))
        assertResult(data)(result)
      }
      it ("should return None on passing an input stream with non zipped data", S3Test) {
        val result = s3.unzipData(unzipped_data_input_stream)
        assertResult(None)(result)
      }
    }

    describe("#delete") {
      it("should return false if no data exists for bucket, key", S3Test) {
        assertResult(false)(s3.delete(nonexistentBucket, test1))
      }

      it("should return true if deleted data exists", S3Test) {
        s3.upload(data_dl, testBucket, test3)
        assertResult(s3.delete(testBucket, test3))(true)
      }
    }


    describe("#deleteBucket") {
      it("should return false if bucket does not exist", S3Test) {
        assertResult(false)(s3.deleteBucket(nonexistentBucket))
      }

      it("should return false if an exception is thrown", S3Test) {
        assertResult(false)(s3.deleteBucket("^^%%"))
      }

      it("should return true if bucket was deleted", S3Test) {
        s3.createBucket(tempTestBucket)
        assertResult(true)(s3.deleteBucket(tempTestBucket))
      }
    }

    describe("#createBucket") {
      it("should return false if bucket already exists", S3Test) {
        assertResult(false)(s3.createBucket(testBucket))
      }

      it("should return false if an exception is thrown", S3Test) {
        assertResult(false)(s3.createBucket("&&@@"))
      }

      it("should return true if bucket was created", S3Test) {
        assertResult(true)(s3.createBucket(tempTestBucket))
      }
    }
  }
}
