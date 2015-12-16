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

import org.scalatest.{BeforeAndAfterAll, FunSpec}

class S3ClientTest extends FunSpec with BeforeAndAfterAll {

  private val testBucket = "uncharted-s3-client-test"
  private val nonexistentBucket = "nonexistent-bucket"
  val tempTestBucket = "uncharted-s3-client-test-temp"

  private val test0 = "test0"
  private val test1 = "test1"
  private val test2 = "test2"

  private val data = Array[Byte](0, 1, 2, 3, 4, 5)
  private val data_dl = Array[Byte](0, 11, 22, 33, 44, 55) // scalastyle:ignore

  private val s3 = S3Client(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))


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
      it("should return true if upload was successful") {
        val result = s3.upload(data, testBucket, test0)
        assertResult(true)(result)
      }

      it("should return false if upload was unsuccessful") {
        val result = s3.upload(data, nonexistentBucket, test0)
        assertResult(false)(result)
      }

      it("should create a S3 object with the supplied bucket, key and data") {
        s3.upload(data, testBucket, test1)
        val result = s3.download(testBucket, test1).getOrElse(fail("Failed to fetch uploaded data"))
        assertResult(data)(result)
      }
    }

    describe("#download") {
      it("should return data corresponding to bucket, key") {
        s3.upload(data_dl, testBucket, test2)
        val result = s3.download(testBucket, test2).getOrElse(fail("Failed to download data"))
        assertResult(data_dl)(result)
      }

      it("should return None if no data exists for bucket, key") {
        val result = s3.download(nonexistentBucket, test1)
        assertResult(None)(result)
      }
    }

    describe("#delete") {
      it("should return false if no data exists for bucket, key") {
        assertResult(false)(s3.delete(nonexistentBucket, test1))
      }

      it("should return true if deleted data exists") {
        s3.upload(data_dl, testBucket, "test3")
        assertResult(s3.delete(testBucket, "test3"))(true)
      }
    }


    describe("#deleteBucket") {
      it("should return false if bucket does not exist") {
        assertResult(false)(s3.deleteBucket(nonexistentBucket))
      }

      it("should return false if an exception is thrown") {
        assertResult(false)(s3.deleteBucket("^^%%"))
      }

      it("should return true if bucket was deleted") {
        s3.createBucket(tempTestBucket)
        assertResult(true)(s3.deleteBucket(tempTestBucket))
      }
    }

    describe("#createBucket") {
      it("should return false if bucket already exists") {
        assertResult(false)(s3.createBucket(testBucket))
      }

      it("should return false if an exception is thrown") {
        assertResult(false)(s3.createBucket("&&@@"))
      }

      it("should return true if bucket was created") {
        assertResult(true)(s3.createBucket(tempTestBucket))
      }
    }
  }
}
